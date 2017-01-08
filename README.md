This is a set of programs to generate and check matrices where there are no two non-empty sets of colums whose column sum is equal.

This is most easily demonstrated with a counterexample:

    0 1 0 0 0 1 0 1 1 1 1 0 0 1 1 0 0 1 0 1 1 0 0
    1 0 1 0 0 0 1 0 1 1 1 1 0 0 1 1 0 0 1 0 1 1 0
    0 1 0 1 0 0 0 1 0 1 1 1 1 0 0 1 1 0 0 1 0 1 1
    0 0 1 0 1 0 0 0 1 0 1 1 1 1 0 0 1 1 0 0 1 0 1
    0 0 0 1 0 1 0 0 0 1 0 1 1 1 1 0 0 1 1 0 0 1 0
    0 0 0 0 1 0 1 0 0 0 1 0 1 1 1 1 0 0 1 1 0 0 1
    0 0 0 0 0 1 0 1 0 0 0 1 0 1 1 1 1 0 0 1 1 0 0
    0 0 0 0 0 0 1 0 1 0 0 0 1 0 1 1 1 1 0 0 1 1 0
    0 0 0 0 0 0 0 1 0 1 0 0 0 1 0 1 1 1 1 0 0 1 1
    0 0 0 0 0 0 0 0 1 0 1 0 0 0 1 0 1 1 1 1 0 0 1
    0 0 0 0 0 0 0 0 0 1 0 1 0 0 0 1 0 1 1 1 1 0 0
    0 0 0 0 0 0 0 0 0 0 1 0 1 0 0 0 1 0 1 1 1 1 0

Putting this matrix in the file `columns.txt` and running

    propertyXCheck < columns.txt

gives as output:

Decomposition: +0 +1 -2 +3 +4 -5 -7 -12 +13 +16 -22

And indeed, if you take the sum of columns 0, 1, 3, 4, 13 and 16 (columns are 0-based starting from the left):

    0 1 0 0 1 0 = 2
    1 0 0 0 0 0   1
    0 1 1 0 0 1   3
    0 0 0 1 1 1   3
    0 0 1 0 1 0   2
    0 0 0 1 1 0   2
    0 0 0 0 1 1   2
    0 0 0 0 0 1   1
    0 0 0 0 1 1   2
    0 0 0 0 0 1   1
    0 0 0 0 0 0   0
    0 0 0 0 0 1   1

Also sum columns 2, 5, 7, 12 and 22:

    0 1 1 0 0 = 2
    1 0 0 0 0   1
    0 0 1 1 1   3
    1 0 0 1 1   3
    0 1 0 1 0   2
    0 0 0 1 1   2
    0 1 1 0 0   2
    0 0 0 1 0   1
    0 0 1 0 1   2
    0 0 0 0 1   1
    0 0 0 0 0   0
    0 0 0 1 0   1

So this matrix is NOT a solution

To look for matrices of N (e.g. 10) rows first create a progress file ending
with `----`. Since initially you know nothing, start with a file containing
just that terminator:

    echo ---- > propertyX.10.txt

Then start a server listening on port 21453 with:

    propertyX 10

(you can listen on another port by giving that as second argument)

Then start any number of clients on the same or different computers:

    propertyX

Optional arguments are host to connect to (default localhost), port to connect to (default 21453) and number of threads (default the same as the number of cores)

Since the default number of threads is by default the same as the number of cores there is usually no point in starting more than 1 client on one host (but you might give an explicit core count and use different clients to work on different row sizes)

If a client is stopped (or crashes or the host goes down) you can just restart it. The starting columns the threads of that client were working are known to the server and it will assign that work to another client if one becomes available (or to the restarted client). You just lose the time the client already spent on the starting columns it was working on.

If the server is stopped (or crashes or the host goes down) the last known results are in the progress file which however is not yet terminated with `----`.
Check the file to make sure it doesn't end on an incomplete line and append a
terminator:

    echo ---- >> propertyX.10.txt

then restart the server. All clients that were connected to the server will have stopped and need to be restarted. All the time the clients spent on their current starting columns is wasted, but these columns will get assigned to the new columns.

Currently known row sizes with the corresponding maximum number of columns:

    2 2
    3 4
    4 5
    5 7
    6 9
    7 12
    8 14
    9 16
    10 18
    11 20
    12 23
    13 25

For example a 12 row matrix can have at most 23 columns. No solutions with 24 or more columns exist. One (of many) solutions:

    rows=12,cols=23
    1 1 1 0 1 0 1 1 0 0 1 0 0 0 1 1 1 1 0 1 0 1 1
    0 1 1 1 0 1 0 1 1 0 0 1 0 0 0 1 1 1 1 0 1 0 1
    0 0 1 1 1 0 1 0 1 1 0 0 1 0 0 0 1 1 1 1 0 1 0
    1 0 0 1 1 1 0 1 0 1 1 0 0 1 0 0 0 1 1 1 1 0 1
    0 1 0 0 1 1 1 0 1 0 1 1 0 0 1 0 0 0 1 1 1 1 0
    0 0 1 0 0 1 1 1 0 1 0 1 1 0 0 1 0 0 0 1 1 1 1
    1 0 0 1 0 0 1 1 1 0 1 0 1 1 0 0 1 0 0 0 1 1 1
    0 1 0 0 1 0 0 1 1 1 0 1 0 1 1 0 0 1 0 0 0 1 1
    1 0 1 0 0 1 0 0 1 1 1 0 1 0 1 1 0 0 1 0 0 0 1
    0 1 0 1 0 0 1 0 0 1 1 1 0 1 0 1 1 0 0 1 0 0 0
    1 0 1 0 1 0 0 1 0 0 1 1 1 0 1 0 1 1 0 0 1 0 0
    1 1 0 1 0 1 0 0 1 0 0 1 1 1 0 1 0 1 1 0 0 1 0
