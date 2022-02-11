GPP = g++

CFLAGS+= -Wfatal-errors -g -Wall -Wformat-nonliteral -Wformat-security -I ./include

CPPFLAGS+= -g -std=c++11 -std=gnu++14 -Wfatal-errors -O1

LDFLAGS +=  -lstdc++ -levent -std=c++11 -std=gnu++14 -lpthread

filemerger_sources = \
		src/main.cpp	\
		src/Worker.cpp

APPS = filemerger 

all: $(APPS)

filemerger: ${filemerger_sources}
	$(GPP) $(CFLAGS) -fPIC $^ -o $@ $(LDFLAGS)

%.o: %.cpp
	$(GPP) $(CFLAGS)  -fPIC -c $< -o $@	

clean:
	rm -f $(APPS)
	rm -f *.o
