#!/bin/bash
# author: Alek Ratzloff
# Runs the spark-signal clean-it-up on an MP3 file.
# (Doesn't actually run on an MP3 file, uses lots of hacks to make it work)

### Functions ###
usage() {
	echo "usage: $0 file1.mp3 [ file2.mp3 ... ]"
}

echo_err() {
	echo -e "\x1b[30;1m[ ERROR ]\x1b[0m "$*
}

echo_warn() {
	echo -e "\x1b[33;1m[ WARN ]\x1b[0m "$*
}

cleanup() {
	for f in $wav_files
	do rm $f
	done
}

### Script variables ###
classname='edu.appstate.cs.SparkSignal'
jarfile='target/spark-signal-1.0-SNAPSHOT.jar'
data_dir='/hdfs/bee'
hdfs_tmp_dir='/hdfs/tmp'
local_tmp_dir='/tmp'
namenode='node1'

### Let's do this ###

# make sure that the user specified arguments
if [[ $# == 0 ]]; then
	usage
	exit 0
fi

# Get the list of wav files that we need to create
mp3_basenames=$@
for f in $mp3_basenames; do
	# make sure it exists
	mp3_path=$data_dir/$f
	if [[ ! -f "$mp3_path" ]]; then
		echo_err "$mp3_path not found, skipping"
		continue
	fi

	wav_basename="${f%.mp3}.wav"
	local_wav_path="$local_tmp_dir/$wav_basename"
	hdfs_wav_path="$hdfs_tmp_dir/$wav_basename"
	wav_hdfs="hdfs://$namenode/${hdfs_wav_path#/hdfs/}"
	echo "$f -> $wav_basename"

	# decode the MP3 to WAV
	mpg123 -w $local_wav_path $mp3_path > /dev/null || \
		{ echo_warn "Could not convert $f, skipping"; continue; }
	mv $local_wav_path $hdfs_wav_path

	wav_files="$wav_files $hdfs_wav_path"
	hdfs_files="$hdfs_files $wav_hdfs"
done

spark-submit --class $classname $jarfile $hdfs_files

# Perform cleanup
echo "Cleaning up..."
cleanup

