module github.com/roncewind/move

go 1.20

require (
	github.com/docktermj/go-xyzzy-helpers v0.2.2
	github.com/rabbitmq/amqp091-go v1.5.0
	github.com/roncewind/go-util v0.0.0-20230209152928-5d0b8b2f7ebd
	github.com/roncewind/szrecord v0.0.6
	github.com/roncewind/workerpool v0.0.1
	github.com/senzing/g2-sdk-go v0.2.4
	github.com/spf13/cobra v1.5.0
	github.com/spf13/viper v1.12.0
)

replace (
	github.com/roncewind/go-util => ../go-util
	github.com/roncewind/move => ../move
)

require (
	github.com/fsnotify/fsnotify v1.5.4 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/magiconair/properties v1.8.6 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/pelletier/go-toml v1.9.5 // indirect
	github.com/pelletier/go-toml/v2 v2.0.1 // indirect
	github.com/senzing/go-logging v1.1.1 // indirect
	github.com/spf13/afero v1.8.2 // indirect
	github.com/spf13/cast v1.5.0 // indirect
	github.com/spf13/jwalterweatherman v1.1.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/subosito/gotenv v1.3.0 // indirect
	golang.org/x/sys v0.0.0-20220520151302-bc2c85ada10a // indirect
	golang.org/x/text v0.3.7 // indirect
	gopkg.in/ini.v1 v1.66.4 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
