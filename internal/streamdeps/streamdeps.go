// Package streamdeps wires the production helper functions into a
// streamrun.Deps — the host side of streamrun's dependency-injection seam.
//
// It is kept OUT of streamrun itself on purpose: streamrun must not import
// metadata/cliutil/byos/config/indexer, or the Deps seam (which exists so
// streamrun stays decoupled from those packages) would be defeated. Both the
// `stream`/`up` commands and the control-plane supervisor call Default() so
// they build identical engine deps.
package streamdeps

import (
	"github.com/dbtrail/bintrail/internal/byos"
	"github.com/dbtrail/bintrail/internal/cliutil"
	"github.com/dbtrail/bintrail/internal/config"
	"github.com/dbtrail/bintrail/internal/indexer"
	"github.com/dbtrail/bintrail/internal/metadata"
	"github.com/dbtrail/bintrail/internal/streamrun"
)

// Default returns the streamrun.Deps used in production.
func Default() streamrun.Deps {
	return streamrun.Deps{
		ValidateBinlogFormat:   metadata.ValidateBinlogFormat,
		ValidateBinlogRowImage: metadata.ValidateBinlogRowImage,
		ValidateNoFKCascades:   metadata.ValidateNoFKCascades,
		ParseSchemaList:        cliutil.ParseSchemaList,
		ResolveServerIdentity:  byos.ResolveServerIdentity,
		EnsureResolver:         metadata.EnsureResolver,
		BuildIndexFilters:      cliutil.BuildIndexFilters,
		InsertSchemaChange:     indexer.InsertSchemaChange,
		ParseSourceDSN:         config.ParseSourceDSN,
		OutputJSON:             cliutil.OutputJSON,
	}
}
