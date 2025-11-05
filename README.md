# Stamper

An experimental project to explore golang's [testing/synctest](https://pkg.go.dev/testing/synctest) package by implementing [viewstamped replication protocol](https://dspace.mit.edu/bitstream/handle/1721.1/71763/MIT-CSAIL-TR-2012-021.pdf?sequence=1&isAllowed=y) and testing it with simulation testing. Code currently is still messy but works as I expected..

### Cmds

To run simulation test
```console
$ GOMAXPROCS=1 go test ./internal/stamper/ -v -count=1 -run TestSimulation
```

### Checklists

VSR
- [x] normal protocol
- [x] view change protocol
- [x] recovery protocol
- [ ] client recovery
- [ ] checkpointing
- [ ] state transfer
- [ ] batching
- [ ] reconfiguration

Simulation testing
- [x] mock time
- [x] mock network
- [x] random recovery
- [ ] fault injection
  - [x] message loss
  - [x] simple network partition
  - [ ] more complex network partition
- [ ] CI to run simulation with random configurations
- [x] end in same state check
- [ ] message loss check
- [ ] split brain check
