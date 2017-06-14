package log_test

import (
	"errors"
	"testing"

	"github.com/ksarch-saas/codis/pkg/utils/log"
)

var path = "/home/yanyu/Workbench/workspace-go/org/src/github.com/ksarch-saas/codis/pkg/utils/log/test"

func TestLogNormal(t *testing.T) {
	log.Init(path, "", log.LstdFlags, log.LEVEL_INFO, log.LEVEL_WARN)

	ss := "XY"
	log.Infof("test: %s", "XY")

	err := errors.New("sorry")
	log.WarnErrorf(err, "my %s", ss)
}
