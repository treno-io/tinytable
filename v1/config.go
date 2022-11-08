package tinytable

import (
	"os"
	"strconv"
)

func coalesce(s ...string) string {
	for _, e := range s {
		if e != "" {
			return e
		}
	}
	return ""
}

type Option func(c Config) Config

type Config struct {
	Debug int
}

func ConfigFromEnv() Config {
	var debug int
	if v := coalesce(os.Getenv("DEBUG"), os.Getenv("DEBUG_TINYTABLE"), os.Getenv("DEBUG_BTX")); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			n = 1
		}
		debug = int(n)
	}
	return Config{
		Debug: debug,
	}
}

func ConfigFromOptions(opts ...Option) Config {
	conf := ConfigFromEnv()
	for _, o := range opts {
		conf = o(conf)
	}
	return conf
}

func Debug(n int) Option {
	return func(c Config) Config {
		c.Debug = n
		return c
	}
}
