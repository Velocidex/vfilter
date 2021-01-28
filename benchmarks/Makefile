all:
	go test -v -bench=. . -benchtime=5s
sync:
	go test -v -bench=. . -benchtime=5s | tee benchmark.txt
