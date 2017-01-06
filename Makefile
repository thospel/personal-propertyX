CXXFLAGS  = -Wall -O3 -march=native -fstrict-aliasing -std=c++14 -g -pthread
LDFLAGS = -g
CXX = ccache g++

all: propertyX propertyXCheck propertyXLatticeCheck

log_buffer.o server.o client.o propertyX.o: log_buffer.hpp
fd_buffer.o server.o: fd_buffer.hpp
server.o client.o propertyX.o: propertyX.hpp

propertyX: propertyX.o server.o client.o fd_buffer.o log_buffer.o
	$(CXX) $(LDFLAGS) -pthread -lev $^ $(LOADLIBES) $(LDLIBS) -o $@

propertyXCheck: propertyXCheck.o
	$(CXX) $(LDFLAGS) $^ $(LOADLIBES) $(LDLIBS) -o $@

propertyXLatticeCheck: propertyXLatticeCheck.o
	$(CXX) $(LDFLAGS) -lntl $^ $(LOADLIBES) $(LDLIBS) -o $@

clean:
	rm -f *.o propertyX propertyXCheck propertyXLatticeCheck
