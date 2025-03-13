package contracts

type Logger interface {
	Log(format string, v ...interface{})
}
