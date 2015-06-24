package actor

type Config struct {
	Servers          []string
	ZookeeperServers []string
	Name             string
	DispatcherPort   int
	DispatcherSize   int
}
