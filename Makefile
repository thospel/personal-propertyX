CXXFLAGS  = -Wall -O3 -march=native -fstrict-aliasing -std=c++14 -g -pthread
LDFLAGS = -g
CXX := ccache $(CXX)

all: propertyX propertyXCheck propertyXLatticeCheck

# propertyX.o: propertyX.hpp
propertyX.o fd_buffer.o server.o client.o: fd_buffer.hpp propertyX.hpp

propertyX: propertyX.o server.o client.o fd_buffer.o
	$(CXX) $(LDFLAGS) -pthread -lev $^ $(LOADLIBES) $(LDLIBS) -o $@

propertyXCheck: propertyXCheck.o
	$(CXX) $(LDFLAGS) $^ $(LOADLIBES) $(LDLIBS) -o $@

propertyXLatticeCheck: propertyXLatticeCheck.o
	$(CXX) $(LDFLAGS) -lntl $^ $(LOADLIBES) $(LDLIBS) -o $@

clean:
	rm -f *.o propertyX propertyXCheck propertyXLatticeCheck
