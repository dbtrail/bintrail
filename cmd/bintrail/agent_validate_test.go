package main

import "testing"

func TestHasReplPrivileges(t *testing.T) {
	tests := []struct {
		name       string
		grants     []string
		wantSlave  bool
		wantClient bool
	}{
		{
			name:       "both privileges",
			grants:     []string{"GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user'@'%'"},
			wantSlave:  true,
			wantClient: true,
		},
		{
			name:       "all privileges",
			grants:     []string{"GRANT ALL PRIVILEGES ON *.* TO 'root'@'localhost'"},
			wantSlave:  true,
			wantClient: true,
		},
		{
			name:       "only slave",
			grants:     []string{"GRANT REPLICATION SLAVE ON *.* TO 'user'@'%'"},
			wantSlave:  true,
			wantClient: false,
		},
		{
			name:       "only client",
			grants:     []string{"GRANT REPLICATION CLIENT ON *.* TO 'user'@'%'"},
			wantSlave:  false,
			wantClient: true,
		},
		{
			name:       "no replication privileges",
			grants:     []string{"GRANT SELECT ON mydb.* TO 'reader'@'%'"},
			wantSlave:  false,
			wantClient: false,
		},
		{
			name: "across multiple grant lines",
			grants: []string{
				"GRANT REPLICATION SLAVE ON *.* TO 'user'@'%'",
				"GRANT REPLICATION CLIENT ON *.* TO 'user'@'%'",
			},
			wantSlave:  true,
			wantClient: true,
		},
		{
			name: "mixed with other privileges",
			grants: []string{
				"GRANT SELECT, INSERT ON mydb.* TO 'user'@'%'",
				"GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user'@'%'",
			},
			wantSlave:  true,
			wantClient: true,
		},
		{
			name:       "empty grants",
			grants:     nil,
			wantSlave:  false,
			wantClient: false,
		},
		{
			name:       "case insensitive",
			grants:     []string{"GRANT replication slave, replication client ON *.* TO 'user'@'%'"},
			wantSlave:  true,
			wantClient: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSlave, gotClient := hasReplPrivileges(tt.grants)
			if gotSlave != tt.wantSlave {
				t.Errorf("slave = %v, want %v", gotSlave, tt.wantSlave)
			}
			if gotClient != tt.wantClient {
				t.Errorf("client = %v, want %v", gotClient, tt.wantClient)
			}
		})
	}
}

func TestValidateFlagRegistration(t *testing.T) {
	f := agentCmd.Flag("validate")
	if f == nil {
		t.Fatal("--validate flag not registered on agent command")
	}
	if f.DefValue != "false" {
		t.Errorf("--validate default = %q, want false", f.DefValue)
	}
}

func TestPrintCheckFormat(t *testing.T) {
	// Verify printCheck and printSkip don't panic.
	// Output goes to stdout which we don't capture, but we verify no crash.
	printCheck("test check", "detail", nil)
	printCheck("test check", "", nil)
	printCheck("test check", "", &testError{"something broke"})
	printSkip("test check", "reason")
}
