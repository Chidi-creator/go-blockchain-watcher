package logger

import (
	"fmt"
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// ANSI color codes
const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorPurple = "\033[35m"
	colorCyan   = "\033[36m"
	colorWhite  = "\033[37m"
)

// Logger is the interface for logging
type Logger interface {
	Debug(msg string, args ...interface{})
	Info(msg string, args ...interface{})
	Warn(msg string, args ...interface{})
	Error(msg string, args ...interface{})
	Fatal(msg string, args ...interface{})
}

// zapLogger implements Logger interface
type zapLogger struct {
	logger *zap.SugaredLogger
}

// NewLogger creates a new Logger instance
func NewLogger(level string) Logger {
	// Parse log level
	var zapLevel zapcore.Level
	switch strings.ToLower(level) {
	case "debug":
		zapLevel = zapcore.DebugLevel
	case "info":
		zapLevel = zapcore.InfoLevel
	case "warn":
		zapLevel = zapcore.WarnLevel
	case "error":
		zapLevel = zapcore.ErrorLevel
	default:
		zapLevel = zapcore.InfoLevel
	}

	// Custom encoder with colored output
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	// Custom console encoder with colors
	consoleEncoder := zapcore.NewConsoleEncoder(encoderConfig)

	// Create the core
	core := zapcore.NewCore(
		consoleEncoder,
		zapcore.AddSync(newColoredWriter()),
		zapLevel,
	)

	// Create the logger
	logger := zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1))
	return &zapLogger{logger: logger.Sugar()}
}

// coloredWriter implements zapcore.WriteSyncer with color support
type coloredWriter struct {
	file *os.File
}

func newColoredWriter() *coloredWriter {
	return &coloredWriter{file: os.Stdout}
}

func (w *coloredWriter) Write(p []byte) (n int, err error) {
	// Get the log message and colorize based on log level
	s := string(p)

	// Apply colors based on log level
	if strings.Contains(s, `"level":"error"`) || strings.Contains(s, `"level":"fatal"`) {
		s = formatLogMessage(s, colorRed)
	} else if strings.Contains(s, `"level":"warn"`) {
		s = formatLogMessage(s, colorYellow)
	} else if strings.Contains(s, `"level":"info"`) {
		s = formatLogMessage(s, colorBlue)
	} else if strings.Contains(s, `"level":"debug"`) {
		s = formatLogMessage(s, colorCyan)
	}

	return w.file.Write([]byte(s))
}

func (w *coloredWriter) Sync() error {
	return w.file.Sync()
}

// formatLogMessage formats the log message to match the desired format
func formatLogMessage(logMessage, color string) string {
	// Extract information from JSON format
	var level, caller, msg string

	// Basic extraction from the JSON format
	if strings.Contains(logMessage, `"level":"`) {
		parts := strings.Split(logMessage, `"level":"`)
		if len(parts) > 1 {
			levelParts := strings.Split(parts[1], `"`)
			if len(levelParts) > 1 {
				level = levelParts[0]
			}
		}
	}

	if strings.Contains(logMessage, `"caller":"`) {
		parts := strings.Split(logMessage, `"caller":"`)
		if len(parts) > 1 {
			callerParts := strings.Split(parts[1], `"`)
			if len(callerParts) > 1 {
				caller = callerParts[0]
			}
		}
	}

	if strings.Contains(logMessage, `"msg":"`) {
		parts := strings.Split(logMessage, `"msg":"`)
		if len(parts) > 1 {
			msgParts := strings.Split(parts[1], `"`)
			if len(msgParts) > 1 {
				msg = msgParts[0]
			}
		}
	}

	// Extract additional fields
	fieldsStr := ""
	keyValuePairs := extractKeyValuePairs(logMessage)
	for k, v := range keyValuePairs {
		if k != "level" && k != "caller" && k != "msg" && k != "time" {
			fieldsStr += fmt.Sprintf("%s=%s ", k, v)
		}
	}

	// Format the log entry in the desired format
	formattedLog := fmt.Sprintf("%s[%s]%s %s --> %s %s\n",
		color,
		strings.ToUpper(level),
		colorReset,
		msg,
		caller,
		fieldsStr,
	)

	return formattedLog
}

// extractKeyValuePairs extracts all key-value pairs from the JSON log message
func extractKeyValuePairs(logMessage string) map[string]string {
	result := make(map[string]string)

	// Simple extraction - this is a basic implementation
	parts := strings.Split(logMessage, `","`)
	for _, part := range parts {
		keyValue := strings.Split(part, `":"`)
		if len(keyValue) == 2 {
			key := strings.TrimPrefix(keyValue[0], `{"`)
			key = strings.TrimPrefix(key, `"`)
			value := strings.TrimSuffix(keyValue[1], `"}`)
			value = strings.TrimSuffix(value, `"`)
			result[key] = value
		}
	}

	return result
}

// Debug logs a debug message
func (l *zapLogger) Debug(msg string, args ...interface{}) {
	l.logger.Debugw(msg, args...)
}

// Info logs an info message
func (l *zapLogger) Info(msg string, args ...interface{}) {
	l.logger.Infow(msg, args...)
}

// Warn logs a warning message
func (l *zapLogger) Warn(msg string, args ...interface{}) {
	l.logger.Warnw(msg, args...)
}

// Error logs an error message
func (l *zapLogger) Error(msg string, args ...interface{}) {
	l.logger.Errorw(msg, args...)
}

// Fatal logs a fatal message and exits
func (l *zapLogger) Fatal(msg string, args ...interface{}) {
	l.logger.Fatalw(msg, args...)
}
