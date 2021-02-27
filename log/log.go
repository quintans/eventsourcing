package log

import "github.com/sirupsen/logrus"

type Tags map[string]interface{}

type Logger interface {
	Debug(args ...interface{})
	Debugf(format string, args ...interface{})
	Info(args ...interface{})
	Infof(format string, args ...interface{})
	Warn(args ...interface{})
	Warnf(format string, args ...interface{})
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
	Fatal(args ...interface{})
	Fatalf(format string, args ...interface{})
	WithTags(tags Tags) Logger
	WithError(err error) Logger
}

type LogrusWrap struct {
	logger *logrus.Entry
}

func NewLogrus(logger *logrus.Logger) LogrusWrap {
	return LogrusWrap{
		logger: logrus.NewEntry(logger),
	}
}

func (l LogrusWrap) Debug(args ...interface{}) {
	logrus.Debug(args...)
}

func (l LogrusWrap) Debugf(format string, args ...interface{}) {
	l.logger.Debugf(format, args...)
}

func (l LogrusWrap) Info(args ...interface{}) {
	l.logger.Info(args...)
}

func (l LogrusWrap) Infof(format string, args ...interface{}) {
	l.logger.Infof(format, args...)
}

func (l LogrusWrap) Warn(args ...interface{}) {
	l.logger.Warn(args...)
}

func (l LogrusWrap) Warnf(format string, args ...interface{}) {
	l.logger.Warnf(format, args...)
}

func (l LogrusWrap) Error(args ...interface{}) {
	l.logger.Error(args...)
}

func (l LogrusWrap) Errorf(format string, args ...interface{}) {
	l.logger.Errorf(format, args...)
}

func (l LogrusWrap) Fatal(args ...interface{}) {
	l.logger.Fatal(args...)
}

func (l LogrusWrap) Fatalf(format string, args ...interface{}) {
	l.logger.Fatalf(format, args...)
}

func (l LogrusWrap) WithError(err error) Logger {
	return l.WithTags(Tags{"error": err})
}

func (l LogrusWrap) WithTags(vals Tags) Logger {
	return LogrusWrap{
		logger: l.logger.WithFields(logrus.Fields(vals)),
	}
}
