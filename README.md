The Idea
=========
The main purpose of this project is to use S3 as a backing grid file system to serve application resources (html, video, etc.) that must go through another server (apache, streaming server) before heading to the client in an extremely fast and low latency way.

The shortcoming of other implementations like `s3fs` are that it introduces too much latency when trying to read files. This attempt is to maintain a low latency file system with practically native throughput at the expense of always having the absolute freshest data. The data is by no means very stale, at most one request will be handled before the new file is copied in.

This program copies an S3 bucket to local storage and attempts to keep the local filesystem in sync with the S3 bucket based on changes occurring to the local system. All updates and checks are done asynchronously and as intelligently as possible. With this method you can have a file system backed by S3 that approaches local FS native speeds.

The current implementation uses fusepy, boto and a threaded Python model. The software is extremely alpha, with limited production testing.

Advantages
----------
* Native Speed (almost)
* Asynchronous S3 checks
* All content is stored locally as well as on S3

Disadvantages
----------
* Eventually Consistent
* All content is stored locally as well as on S3

Trying it Out
=========
You'll need fusepy and boto, both installable via `pip` or `easy_install`.

    pip install fusepy boto

The python executable will mount the file system.

    ./pyfasts3.py [options] <AWS_KEY> <AWK_SECRET> <local-cache> <mount-point>


Plans
=========
The thing that isn't done here yet is to make a notification system of some kind that can alert peers when files change. That way mass invalidations and updates can occur immediately without needing to wait for stale requests to be served.


Helping
=========
Yep, that would be amazing. Look it over, fork it, submit patches, I'm around. I think this project has potential and I am open to any changes as long as the Idea remains the same.
