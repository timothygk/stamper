# Stamper

An experimental project to explore golang's [testing/synctest](https://pkg.go.dev/testing/synctest) package by implementing [viewstamped replication protocol](https://dspace.mit.edu/bitstream/handle/1721.1/71763/MIT-CSAIL-TR-2012-021.pdf?sequence=1&isAllowed=y) and testing it with simulation testing. Code currently is still messy but works as I expected..

### Cmds

To run simulation test
```console
$ GOMAXPROCS=1 go test ./internal/stamper/ -v -count=1 -run TestSimulation
```

random simulation testing
```console
$ GOMAXPROCS=1 go test ./internal/stamper/ -v -count=1 -run TestRandomSimulation
```

### Checklists

VSR
- [x] normal protocol
- [x] view change protocol
- [x] recovery protocol
- [ ] client recovery
- [ ] checkpointing
- [x] state transfer
- [x] suffix based view change
- [ ] batching
- [ ] reconfiguration

Simulation testing
- [ ] simulation
  - [x] mock time
  - [x] mock network
  - [x] out-of-order delivery
  - [x] simple network delay
  - [ ] more complex network delay
  - [ ] non-uniform configuration
  - [ ] simulator config random generator
  - [ ] CI to run simulation with random configurations
- [ ] fault injection
  - [x] message loss
  - [x] simple network partition
  - [ ] more complex network partition
  - [x] random recovery
- [ ] invariants
  - [x] reply correctness check
  - [x] eventually ended in the same state check
  - [x] message loss check
  - [x] split brain check
