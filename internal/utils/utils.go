package utils

import (
	"time"
)

func FromTimeToDateStr(t time.Time) string {
	return t.Format("2006-01-02") // YYYY-MM-DD. ref: https://pkg.go.dev/time#Time.Format
}

func FromDateStrToTime(dateStr string) (time.Time, error) {
	return time.Parse("2006-01-02", dateStr)
}
