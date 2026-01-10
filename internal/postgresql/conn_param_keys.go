package postgresql

// TODO: implement all other options as well

// ConnParamKey is an enum for PostgreSQL connection parameters
type ConnParamKey string

const (
	// ConnParamKeyHost defines the key for the hostname
	ConnParamKeyHost ConnParamKey = "host"
	// ConnParamKeyPort defines the key for the port
	ConnParamKeyPort ConnParamKey = "port"
	// ConnParamKeyUser defines the key for the username
	ConnParamKeyUser ConnParamKey = "user"
	// ConnParamKeyAppName defines the key for the application name
	ConnParamKeyAppName ConnParamKey = "application_name"
	// ConnParamKeySSLMode defines the key for the ssl mode
	ConnParamKeySSLMode ConnParamKey = "sslmode"
)
