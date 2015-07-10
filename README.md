# spark-signal
Signal processing using Apache Spark

Requires Java JDK 1.7.0 or higher.

spark-signal is currently a work in progress for cleaning up audio data collected from beehives.

Here is a goal checksheet for what needs to be done to achieve alpha status:
~~* Load wav files as RDDs~~
* Generate a high-pass filter using RDDs
* Apply filters using RDDs

This is a wishlist of things that would be nice to have. They aren't critical to the application:
* Load MP3 files as RDDs

# License
GPL v3

# Acknowledgements
Thanks to Dr. Andy Greensted for the Java WavFile library. His website is http://www.labbookpages.co.uk, and it contains other useful software snippets that he's accumulated and put together over the years.
