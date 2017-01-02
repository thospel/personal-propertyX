CXXFLAGS  = -Wall -O3 -march=native -fstrict-aliasing -std=c++14 -g -pthread
LDFLAGS = -g
CXX = g++

all: propertyX propertyXCheck propertyXLatticeCheck

propertyX: propertyX.o
	$(CXX) $(LDFLAGS) -pthread -lev $^ $(LOADLIBES) $(LDLIBS)

propertyXCheck: propertyXCheck.o
	$(CXX) $(LDFLAGS) $^ $(LOADLIBES) $(LDLIBS)

propertyXLatticeCheck: propertyXLatticeCheck.o
	$(CXX) $(LDFLAGS) -lntl $^ $(LOADLIBES) $(LDLIBS)

clean:
	rm *.o propertyX propertyXCheck propertyXLatticeCheck
