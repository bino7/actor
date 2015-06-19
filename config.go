package actor

type Config struct {
	servers          []string
	zookeeperServers []string
	tickTime         string
	name             string
	dispatcher_port  int
	dispatcher_size  int
}
