// CloudWatch Logs audit source — new in bintrail, no SaaS ancestor.
//
// RDS and Aurora can export audit logs to CloudWatch Logs, and AWS
// recommends reading them there rather than through the RDS file API:
// DownloadDBLogFilePortion is served by the DB instance itself and consumes
// its resources, while CloudWatch reads never touch the instance. Log
// groups:
//
//   - RDS instance:   /aws/rds/instance/<db-id>/audit
//   - Aurora cluster: /aws/rds/cluster/<cluster-id>/audit
//     (one log stream per instance in the cluster)
//
// Events are fetched with FilterLogEvents across ALL streams of the group,
// which also sidesteps the Aurora striped-file quirk — CloudWatch ingests
// the stripes into per-instance streams and the read below re-sorts by
// record timestamp anyway.
//
// Timestamp semantics: a CloudWatch event's own Timestamp tracks log
// delivery, which lags the audit record's embedded timestamp by an
// undocumented amount. It is therefore used only as a coarse pre-filter —
// the Since/Until window widened by cwlWindowPad — and the exact window is
// enforced on the record-embedded timestamp during parsing. Events are
// re-sorted by record timestamp before offset/limit paging.
//
// Required IAM action: logs:FilterLogEvents (scope to the log group ARN).
package forensics

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	smithy "github.com/aws/smithy-go"

	"github.com/dbtrail/dbtrail/internal/storage"
)

// ErrAuditLogGroupNotFound means the CloudWatch log group derived from the
// host (or given explicitly) does not exist in the region.
var ErrAuditLogGroupNotFound = errors.New("CloudWatch log group not found")

// cwlWindowPad widens the Since/Until window applied to CloudWatch event
// timestamps. Exact filtering happens on the record-embedded timestamps.
const cwlWindowPad = 5 * time.Minute

// cwlAPI is the subset of the CloudWatch Logs client used by the audit
// source. Narrow interface for testability, following the storage.s3API
// pattern.
type cwlAPI interface {
	FilterLogEvents(ctx context.Context, params *cloudwatchlogs.FilterLogEventsInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.FilterLogEventsOutput, error)
}

// newCWLClient builds the real CloudWatch Logs client. Package-level hook
// so unit tests can inject a mock. All config/region resolution goes
// through storage.LoadAWSConfig (EC2/ECS IMDS region fallback, #697).
var newCWLClient = func(ctx context.Context, region string) (cwlAPI, error) {
	cfg, err := storage.LoadAWSConfig(ctx, region)
	if err != nil {
		return nil, err
	}
	return cloudwatchlogs.NewFromConfig(cfg), nil
}

// resolveCWLogGroup determines the log group and region for the CloudWatch
// source: an explicit CloudWatchLogGroup wins; otherwise the group is
// derived from the RDS endpoint shape of SourceHost (cluster endpoints —
// "cluster-" middle label — map to the cluster-level group Aurora exports
// to; instance endpoints to the instance group).
func resolveCWLogGroup(opts AuditReadOptions) (group, region string, err error) {
	host := normalizeAuditHost(opts.SourceHost)
	id, hostRegion, isRDS := parseRDSHost(host)

	if opts.CloudWatchLogGroup != "" {
		// Region still comes from the host when it is RDS-shaped; otherwise
		// the SDK chain (env, shared config, IMDS) resolves it.
		return opts.CloudWatchLogGroup, hostRegion, nil
	}
	if !isRDS {
		return "", "", fmt.Errorf(
			"audit source %q requires an RDS endpoint host in SourceHost to derive the log group, "+
				"or an explicit CloudWatchLogGroup (e.g. /aws/rds/cluster/<cluster-id>/audit); got host %q",
			AuditSourceCloudWatch, opts.SourceHost)
	}
	if isRDSClusterEndpoint(host) {
		return "/aws/rds/cluster/" + id + "/audit", hostRegion, nil
	}
	return "/aws/rds/instance/" + id + "/audit", hostRegion, nil
}

// classifyCWLError maps AWS SDK errors from FilterLogEvents to the
// package's sentinel errors with actionable remediation.
func classifyCWLError(err error, logGroup string) error {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		switch {
		case code == "ResourceNotFoundException":
			return fmt.Errorf(
				"%w: %q — enable audit log export to CloudWatch on the instance/cluster "+
					"(RDS console → Log exports → Audit log), and note Aurora exports to the CLUSTER-level "+
					"group /aws/rds/cluster/<cluster-id>/audit; set CloudWatchLogGroup explicitly if the "+
					"derived name is wrong",
				ErrAuditLogGroupNotFound, logGroup)
		case code == "AccessDenied" || code == "AccessDeniedException" ||
			code == "UnauthorizedOperation":
			return fmt.Errorf(
				"%w: add logs:FilterLogEvents to the IAM policy (resource: the %q log group ARN)",
				ErrAuditAccessDenied, logGroup)
		case code == "Throttling" || code == "ThrottlingException" ||
			code == "RequestLimitExceeded" || code == "TooManyRequestsException":
			return fmt.Errorf(
				"%w: CloudWatch Logs throttled while reading %q; retry after a few seconds",
				ErrAuditThrottled, logGroup)
		default:
			return fmt.Errorf("CloudWatch audit log read failed: %s (%s)", apiErr.ErrorMessage(), code)
		}
	}

	if isAWSCredentialError(err) {
		return fmt.Errorf(
			"%w: attach an IAM role or configure credentials with logs:FilterLogEvents to read "+
				"audit logs from CloudWatch",
			ErrAuditNoAWSCredentials)
	}

	return fmt.Errorf("CloudWatch audit log read failed: %w", err)
}

// readAuditCloudWatch serves AuditSourceCloudWatch. It needs no database
// connection: the log group is derived from the host (or given), and the
// records are always the MariaDB server_audit dialect — the only format
// RDS/Aurora export to CloudWatch.
func readAuditCloudWatch(ctx context.Context, opts AuditReadOptions) (AuditReadResult, error) {
	res := AuditReadResult{
		Source:         AuditSourceCloudWatch,
		Variant:        AuditVariantMariaDB,
		FormatDetected: AuditFormatMariaDB,
	}

	group, region, err := resolveCWLogGroup(opts)
	if err != nil {
		return res, err
	}
	res.FilePath = group

	client, err := newCWLClient(ctx, region)
	if err != nil {
		return res, classifyCWLError(err, group)
	}

	input := &cloudwatchlogs.FilterLogEventsInput{LogGroupName: &group}
	if !opts.Since.IsZero() {
		st := opts.Since.Add(-cwlWindowPad).UnixMilli()
		input.StartTime = &st
	}
	if !opts.Until.IsZero() {
		et := opts.Until.Add(cwlWindowPad).UnixMilli()
		input.EndTime = &et
	}

	filter := auditLogFilter{
		since:     opts.Since,
		until:     opts.Until,
		user:      opts.User,
		eventType: opts.EventType,
	}

	streams := map[string]struct{}{}
	var events []AuditEvent
	localTimeSeen := false
	capped := false

scan:
	for {
		out, ferr := client.FilterLogEvents(ctx, input)
		if ferr != nil {
			res.FilesRead = len(streams)
			return res, classifyCWLError(ferr, group)
		}
		for _, e := range out.Events {
			if e.Message == nil {
				continue
			}
			if e.LogStreamName != nil {
				streams[*e.LogStreamName] = struct{}{}
			}
			// One CloudWatch event usually carries one audit record, but
			// tolerate batched multi-line messages.
			for line := range strings.SplitSeq(*e.Message, "\n") {
				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}
				ev, localTime, ok := parseMariaDBLine(line)
				if !ok {
					res.SkippedLines++
					continue
				}
				localTimeSeen = localTimeSeen || localTime
				res.TotalScanned++
				// Exact window enforcement on the record-embedded timestamp;
				// the padded StartTime/EndTime above is only a coarse
				// pre-filter over delivery timestamps.
				if !filter.matches(&ev) {
					continue
				}
				events = append(events, ev)
				if len(events) >= maxEventsPerFile {
					capped = true
					break scan
				}
			}
		}
		if out.NextToken == nil || *out.NextToken == "" {
			break
		}
		input.NextToken = out.NextToken
	}

	if localTimeSeen {
		res.Warnings = appendUniqueWarnings(res.Warnings, []string{mariadbLocalTimeNote})
	}
	if capped {
		res.Warnings = append(res.Warnings, fmt.Sprintf(
			"matched-event cap (%d) reached while reading %s; narrow since/until", maxEventsPerFile, group))
	}

	// Multi-stream interleave (one stream per Aurora instance) gives no
	// cross-stream ordering — sort by record timestamp, then page.
	sortAuditEventsByTimestamp(events)
	if opts.Offset > 0 {
		if opts.Offset >= len(events) {
			events = nil
		} else {
			events = events[opts.Offset:]
		}
	}
	if len(events) > opts.Limit {
		events = events[:opts.Limit]
	}
	res.Events = events
	res.FilesRead = len(streams)
	return res, nil
}
