package statistics

import "time"

type StatHolder interface {
	// add quantile to stat holder
	Add(statType StatisticsType, value float64) error

	RecordStartTime(statType StatisticsType, t time.Time)
	GetTimeQuantile(statType StatisticsType, q float64) float64
	GetTimeData() *StartTimes
}
