JDKPATH = /usr
LIBPATH = "lib/bufmgr.jar:lib/diskmgr.jar:lib/heap.jar:lib/index.jar"

CLASSPATH = .:..:$(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac 
JAVA  = $(JDKPATH)/bin/java 

PROGS = xx

all: $(PROGS)

compile:src/*/*.java
	$(JAVAC) -cp $(CLASSPATH) -d bin src/*/*.java

xx : compile
	$(JAVA) -cp $(CLASSPATH):bin tests.QEPTest

clean:
	$(RM) -r bin/global bin/relop bin/tests    
