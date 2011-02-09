default: http-overload
.dummy: clean default


http-overload: http-overload.c
	gcc -O3 -g --std=c99 -o $@ $^


clean:
	rm -f http-overload
