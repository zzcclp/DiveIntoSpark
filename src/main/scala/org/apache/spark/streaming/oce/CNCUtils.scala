/**
  * @author zhangzc
  * Apr 13, 2016 5:55:45 PM
  * TODO :
  *
  */
package org.apache.spark.streaming.oce

import scala.reflect.ClassTag

import org.apache.hadoop.io.{BytesWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}

import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._

import org.apache.spark.streaming.oce.dstream.NonTimingUpdateFileInputDStream

/**
  * @author zhangzc
  * Apr 13, 2016 5:55:45 PM
  * TODO :
  *
  */
object CNCUtils {
  
  /**
   * Create a input stream that monitors a Hadoop-compatible filesystem
   * for new files and reads them using the given key-value types and input format.
   * Files must be written to the monitored directory by "moving" them from another
   * location within the same file system. File names starting with . are ignored.
   * 
   * @param ssc StreamingContext object
   * @param directory HDFS directory to monitor for new file
   * @tparam K Key type for reading HDFS file
   * @tparam V Value type for reading HDFS file
   * @tparam F Input format for reading HDFS file
   */
  def nonTimingUpdateFileInputDStream[
    K: ClassTag,
    V: ClassTag,
    F <: NewInputFormat[K, V]: ClassTag
  ] (ssc: StreamingContext, directory: String): InputDStream[(K, V)] = {
    new NonTimingUpdateFileInputDStream[K, V, F](ssc, directory)
  }
  
  /**
   * Create a input stream that monitors a Hadoop-compatible filesystem
   * for new files and reads them as text files (using key as LongWritable, value
   * as Text and input format as TextInputFormat). Files must be written to the
   * monitored directory by "moving" them from another location within the same
   * file system. File names starting with . are ignored.
   * 
   * @param ssc StreamingContext object
   * @param directory HDFS directory to monitor for new file
   */
  def textNonTimingUpdateFileInputDStream(ssc: StreamingContext, directory: String) = {
    ssc.withNamedScope("NonTimingUpdateFileInputDStream"){
      nonTimingUpdateFileInputDStream[LongWritable, Text, TextInputFormat](ssc, directory).map(_._2.toString)
    }
  }
  
}