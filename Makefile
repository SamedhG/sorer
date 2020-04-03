build:
	docker build -t sorer .
	docker run -it -v `pwd`:/sorer sorer cargo build --release
	cp target/release/sorer .

run:
	docker run -it -v `pwd`:/sorer sorer ./sorer -f tests/sor.txt -print_col_type 0   

test:
	docker run -it -v `pwd`:/sorer sorer cargo test

bash:
	docker run -it -v `pwd`:/sorer sorer bash

clean:
	docker run -it -v `pwd`:/sorer sorer cargo clean
	- rm sorer
	- rm -r doc
doc:
	docker run -it -v `pwd`:/sorer sorer cargo doc --no-deps 
	cp -r target/doc .
