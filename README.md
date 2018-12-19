# Benchmark MySQL

This is a client of [fperf](https://github.com/fperf/fperf) which benchmarks MySQL

## How to use

### Build with fperf-build

```
go install github.com/fperf/fperf/bin/fperf-build
fperf-build github.com/fperf/mysql
```

### Run benchmarks

```
./fperf -goroutine 512 -server 'root@tcp(mysql.example.com:3306)/fperf' mysql 'your sqls'
```
