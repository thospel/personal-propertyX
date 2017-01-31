CXXFLAGS  = -Wall -O3 -march=native -fstrict-aliasing -std=c++14 -g -pthread
LDFLAGS = -g
CXX := ccache $(CXX)

all: propertyX propertyXCheck propertyXLatticeCheck avxtest

# propertyX.o: propertyX.hpp
fd_buffer.o server.o: fd_buffer.hpp
log_buffer.o propertyX.o server.o client.o: log_buffer.hpp
propertyX.o server.o client.o: propertyX.hpp

propertyX: propertyX.o client.o log_buffer.o server.o fd_buffer.o
	$(CXX) $(LDFLAGS) -pthread -lev $^ $(LOADLIBES) $(LDLIBS) -o $@

propertyXCheck: propertyXCheck.o
	$(CXX) $(LDFLAGS) $^ $(LOADLIBES) $(LDLIBS) -o $@

propertyXLatticeCheck: propertyXLatticeCheck.o
	$(CXX) $(LDFLAGS) -lntl $^ $(LOADLIBES) $(LDLIBS) -o $@

clean:
	rm -f *.o propertyX propertyXCheck propertyXLatticeCheck
