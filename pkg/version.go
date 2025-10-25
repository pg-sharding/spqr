package pkg

import "fmt"

var (
	// These variables are here only to show current version. They are set in makefile during build process
	SpqrVersion         = "devel"
	GitCommitCount      = "0"
	GitRevision         = "devel"
	SpqrVersionRevision = fmt.Sprintf("%s-%s-%s", SpqrVersion, GitCommitCount, GitRevision)
)
