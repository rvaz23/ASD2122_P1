# asd2122

This is the source code provided to bootstrap the first phase of the Algorithms and Distributed Systems 21/22 at NOVA School for Science and Technology.

The source code should not be used for any other purpose other than the realization of project above without the explicity autorization of the author.


mvn package

java -jar target/asdProj.jar -conf babel_config.properties address=localhost port=9999 contact=192.168.0.12:9998 my_index=1 | tee results/results-localhost-1.txt

