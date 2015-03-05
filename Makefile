JDKPATH = /usr
LIBPATH = lib/heapAssign.jar

CLASSPATH = .:..:$(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac 
JAVA  = $(JDKPATH)/bin/java 

PROGS = mine

all: $(PROGS)

compile:src/*/*.java
	$(JAVAC) -cp $(CLASSPATH) -d bin src/*/*.java

xx : compile
	$(JAVA) -cp $(CLASSPATH):bin tests.HFTest

mine : compile
	$(JAVA) -cp $(CLASSPATH):bin tests.MyTest

clean:
	$(RM) -r bin/heap bin/tests
