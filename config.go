package simpledb

type Config struct {
	withExpired bool
}

type DBOption func(o Config) Config

func DBOptionWithExpired() DBOption {
	return func(o Config) Config {
		o.withExpired = true
		return o
	}
}

type SaveOptions struct {
	isExpired bool
	ttl       int64
}

type SaveOption func(o SaveOptions) SaveOptions

func SaveOptionTTL(ttl int64) SaveOption {
	return func(o SaveOptions) SaveOptions {
		if ttl <= 0 {
			return o
		}
		o.isExpired = true
		o.ttl = ttl
		return o
	}
}
