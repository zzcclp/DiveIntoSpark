/**
  * @author zhangzc
  * Apr 13, 2016 5:59:15 PM
  * TODO :
  *
  */
package org.apache.spark.streaming.oce.dstream

import java.io.{IOException, ObjectInputStream}

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}

import org.apache.spark.rdd.{RDD, UnionRDD, BlockRDD}
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.scheduler.StreamInputInfo
import org.apache.spark.util.{SerializableConfiguration, TimeStampedHashMap, Utils}

/**
  * @author zhangzc
  * Apr 13, 2016 5:59:15 PM
  * TODO :
  *
  */
class NonTimingUpdateFileInputDStream[K, V, F <: NewInputFormat[K, V]](
    ssc_ : StreamingContext,
    directory: String,
    filter: Path => Boolean = FileInputDStream.defaultFilter,
    newFilesOnly: Boolean = true,
    conf: Option[Configuration] = None)
    (implicit km: ClassTag[K], vm: ClassTag[V], fm: ClassTag[F])
  extends InputDStream[(K, V)](ssc_) {
  
  private val serializableConfOpt = conf.map(new SerializableConfiguration(_))

  /**
   * Minimum duration of remembering the information of selected files. Defaults to 60 seconds.
   *
   * Files with mod times older than this "window" of remembering will be ignored. So if new
   * files are visible within this window, then the file will get selected in the next batch.
   */
  private val minRememberDurationS = {
    Seconds(ssc.conf.getTimeAsSeconds("spark.streaming.nonTimingUpdateFileStream.minRememberDuration",
      ssc.conf.get("spark.streaming.minRememberDuration", "60s")))
  }

  // This is a def so that it works during checkpoint recovery:
  private def clock = ssc.scheduler.clock

  // Data to be saved as part of the streaming checkpoints
  protected[streaming] override val checkpointData = new NonTimingUpdateFileInputDStreamCheckpointData

  // Initial ignore threshold based on which old, existing files in the directory (at the time of
  // starting the streaming application) will be ignored or considered
  private val initialModTimeIgnoreThreshold = if (newFilesOnly) clock.getTimeMillis() else 0L

  /*
   * Make sure that the information of files selected in the last few batches are remembered.
   * This would allow us to filter away not-too-old files which have already been recently
   * selected and processed.
   */
  private val numBatchesToRemember = FileInputDStream
    .calculateNumBatchesToRemember(slideDuration, minRememberDurationS)
  private val durationToRemember = slideDuration * numBatchesToRemember
  remember(durationToRemember)

  // Map of batch-time to selected file info for the remembered batches
  // This is a concurrent map because it's also accessed in unit tests
  @transient private[streaming] var batchTimeToSelectedFiles =
    new mutable.HashMap[Time, Array[String]] with mutable.SynchronizedMap[Time, Array[String]]

  // Set of files that were selected in the remembered batches
  @transient private var recentlySelectedFiles = new mutable.HashSet[String]()

  // Read-through cache of file mod times, used to speed up mod time lookups
  @transient private var fileToModTime = new TimeStampedHashMap[String, Long](true)

  // Timestamp of the last round of finding files
  @transient private var lastNewFileFindingTime = 0L

  @transient private var path_ : Path = null
  @transient private var fs_ : FileSystem = null

  override def start() { }

  override def stop() { }
  
  // Map of batch-time to new files's time info for the remembered batches
  @transient private[streaming] var batchTimeToNewFilesTime =
    new mutable.HashMap[Time, Time] with mutable.SynchronizedMap[Time, Time]
  
  // Batch Time of the last round of having new files
  @transient private[streaming] var lastNewFileFindedTime: Time = null

  /**
   * Finds the files that were modified since the last time this method was called and makes
   * a union RDD out of them. Note that this maintains the list of files that were processed
   * in the latest modification time in the previous call to this method. This is because the
   * modification time returned by the FileStatus API seems to return times only at the
   * granularity of seconds. And new files may have the same modification time as the
   * latest modification time in the previous call to this method yet was not reported in
   * the previous call.
   */
  override def compute(validTime: Time): Option[RDD[(K, V)]] = {
    // Find new files
    val newFiles = findNewFiles(validTime.milliseconds)
    logInfo("New files at time " + validTime + ":\n" + newFiles.mkString("\n"))
    
    // 有新文件则生成新RDD. 特殊情况: 如果第一次batch time没有新文件,也需要生成一个空RDD
    val rdds = {
      if (newFiles.size > 0 || (validTime - slideDuration) <= zeroTime){
        batchTimeToSelectedFiles += ((validTime, newFiles))
        recentlySelectedFiles ++= newFiles
      
        lastNewFileFindedTime = validTime
        batchTimeToNewFilesTime += ((validTime, lastNewFileFindedTime))
      
        Some(filesToRDD(newFiles))
      }
      else { // 未找到新文件, 则采用上次batch time的RDD作为此次batch time的RDD
        val prevTime = validTime - slideDuration
        val prevFiles = batchTimeToSelectedFiles.get(prevTime).get
        batchTimeToSelectedFiles += ((validTime, prevFiles))
        batchTimeToNewFilesTime += ((validTime, lastNewFileFindedTime))
        generatedRDDs.get(prevTime)
      }
    }
    
    // Copy newFiles to immutable.List to prevent from being modified by the user
    val metadata = Map(
      "files" -> newFiles.toList,
      StreamInputInfo.METADATA_KEY_DESCRIPTION -> newFiles.mkString("\n"))
    val inputInfo = StreamInputInfo(id, 0, metadata)
    ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)
    
    // logging
    logDebug(validTime.toString() + "===============compute===========================")
    logDebug("batchTimeToSelectedFiles:\n" +
      batchTimeToSelectedFiles.map(p => (p._1, p._2.mkString(", "))).mkString("\n"))
    logDebug("recentlySelectedFiles:\n" + recentlySelectedFiles.mkString("\n"))
    logDebug("lastNewFileFindingTime:\n" + lastNewFileFindingTime.toString())
    logDebug("lastNewFileFindedTime:\n" + lastNewFileFindedTime.toString())
    logDebug("batchTimeToNewFilesTime:\n" +
      batchTimeToNewFilesTime.map(p => (p._1, p._2)).mkString("\n"))
    logDebug(validTime.toString() + "===============compute===========================")
    rdds
  }

  /** Clear the old time-to-files mappings along with old RDDs */
  protected[streaming] override def clearMetadata(time: Time) {
    val unpersistData = ssc.conf.getBoolean("spark.streaming.unpersist", true)
    
    /*
     *                                          rememberTime
     *                     time - rememberDuration  |
     *               lastNewFilesTime       |       |
     *                      |               |       |
     *                      |               |       |
     *                      |               |       |   currentTime
     *       T1			T2			T3			T4			T5			T6			T7			T8
     *      rdd0   rdd0    rdd1    rdd1    rdd1    rdd1    rdd2    
     * 
     * case2:
     *                                          rememberTime
     *                     time - rememberDuration  |
     *                                         lastNewFilesTime     
     *                                 removeTime   |
     *                                      |       |
     *                                      |       |   currentTime
     *       T1			T2			T3			T4			T5			T6			T7			T8
     *      rdd0   rdd0    rdd1    rdd1    rdd2    rdd2    rdd2    
     * 
     */
    // remember的最后一个batch time
    val rememberTime = time - rememberDuration + slideDuration
    // 获取remember的最后一个batch time对应的new files 的batch time
    val lastNewFilesTime = batchTimeToNewFilesTime.getOrElse(rememberTime, zeroTime)
        
    val oldRDDs = batchTimeToNewFilesTime.filter(_._1 <= (time - rememberDuration))
    val removedKeys = oldRDDs.map(e => (e._2, e._1)).filter { _._1 < lastNewFilesTime }
    
    logDebug("Clearing references time=" + (time - rememberDuration).toString() + " to old RDDs and old files: ")
    
    removedKeys.foreach { (t: (Time, Time)) =>
      if (unpersistData) {
        logDebug("Unpersisting old RDDs: " + generatedRDDs.get(t._2).get.id)
        generatedRDDs.get(t._2).get.unpersist(false)
        generatedRDDs.get(t._2).get match {
          case b: BlockRDD[_] =>
            logInfo("Removing blocks of RDD " + b + " of time " + time)
            b.removeBlocks()
          case _ =>
        }
      }
      logDebug("Cleared files are:\n" +
        batchTimeToSelectedFiles.get(t._2).get.mkString("\n"))
      recentlySelectedFiles --= batchTimeToSelectedFiles.get(t._2).get
    }
    generatedRDDs --= oldRDDs.keys
    dependencies.foreach(_.clearMetadata(time))
    
    batchTimeToSelectedFiles --= oldRDDs.keys
    batchTimeToNewFilesTime --= oldRDDs.keys
    // Delete file mod times that weren't accessed in the last round of getting new files
    fileToModTime.clearOldValues(lastNewFileFindingTime - 1)
    
    // logging
    logDebug(time.toString() + "===============clearMetadata===========================")
    //logDebug("removeTime:\n" + removeTime.toString())
    logDebug("batchTimeToSelectedFiles:\n" +
      batchTimeToSelectedFiles.map(p => (p._1, p._2.mkString(", "))).mkString("\n"))
    logDebug("recentlySelectedFiles:\n" + recentlySelectedFiles.mkString("\n"))
    logDebug("lastNewFileFindingTime:\n" + lastNewFileFindingTime.toString())
    logDebug("lastNewFileFindedTime:\n" + lastNewFileFindedTime.toString())
    logDebug("batchTimeToNewFilesTime:\n" +
      batchTimeToNewFilesTime.map(p => (p._1, p._2)).mkString("\n"))
    logDebug(time.toString() + "===============clearMetadata===========================")
    
  }

  /**
   * Find new files for the batch of `currentTime`. This is done by first calculating the
   * ignore threshold for file mod times, and then getting a list of files filtered based on
   * the current batch time and the ignore threshold. The ignore threshold is the max of
   * initial ignore threshold and the trailing end of the remember window (that is, which ever
   * is later in time).
   */
  private def findNewFiles(currentTime: Long): Array[String] = {
    try {
      lastNewFileFindingTime = clock.getTimeMillis()

      // Calculate ignore threshold
      val modTimeIgnoreThreshold = math.max(
        initialModTimeIgnoreThreshold,   // initial threshold based on newFilesOnly setting
        currentTime - durationToRemember.milliseconds  // trailing end of the remember window
      )
      logDebug(s"Getting new files for time $currentTime, " +
        s"ignoring files older than $modTimeIgnoreThreshold")
      val filter = new PathFilter {
        def accept(path: Path): Boolean = isNewFile(path, currentTime, modTimeIgnoreThreshold)
      }
      val newFiles = fs.listStatus(directoryPath, filter).map(_.getPath.toString)
      val timeTaken = clock.getTimeMillis() - lastNewFileFindingTime
      logInfo("Finding new files took " + timeTaken + " ms")
      logDebug("# cached file times = " + fileToModTime.size)
      if (timeTaken > slideDuration.milliseconds) {
        logWarning(
          "Time taken to find new files exceeds the batch size. " +
            "Consider increasing the batch size or reducing the number of " +
            "files in the monitored directory."
        )
      }
      newFiles
    } catch {
      case e: Exception =>
        logWarning("Error finding new files", e)
        reset()
        Array.empty
    }
  }

  /**
   * Identify whether the given `path` is a new file for the batch of `currentTime`. For it to be
   * accepted, it has to pass the following criteria.
   * - It must pass the user-provided file filter.
   * - It must be newer than the ignore threshold. It is assumed that files older than the ignore
   *   threshold have already been considered or are existing files before start
   *   (when newFileOnly = true).
   * - It must not be present in the recently selected files that this class remembers.
   * - It must not be newer than the time of the batch (i.e. `currentTime` for which this
   *   file is being tested. This can occur if the driver was recovered, and the missing batches
   *   (during downtime) are being generated. In that case, a batch of time T may be generated
   *   at time T+x. Say x = 5. If that batch T contains file of mod time T+5, then bad things can
   *   happen. Let's say the selected files are remembered for 60 seconds.  At time t+61,
   *   the batch of time t is forgotten, and the ignore threshold is still T+1.
   *   The files with mod time T+5 are not remembered and cannot be ignored (since, t+5 > t+1).
   *   Hence they can get selected as new files again. To prevent this, files whose mod time is more
   *   than current batch time are not considered.
   */
  private def isNewFile(path: Path, currentTime: Long, modTimeIgnoreThreshold: Long): Boolean = {
    val pathStr = path.toString
    // Reject file if it does not satisfy filter
    if (!filter(path)) {
      logDebug(s"$pathStr rejected by filter")
      return false
    }
    // Reject file if it was created before the ignore time
    val modTime = getFileModTime(path)
    if (modTime <= modTimeIgnoreThreshold) {
      // Use <= instead of < to avoid SPARK-4518
      logDebug(s"$pathStr ignored as mod time $modTime <= ignore time $modTimeIgnoreThreshold")
      return false
    }
    // Reject file if mod time > current batch time
    if (modTime > currentTime) {
      logDebug(s"$pathStr not selected as mod time $modTime > current time $currentTime")
      return false
    }
    // Reject file if it was considered earlier
    if (recentlySelectedFiles.contains(pathStr)) {
      logDebug(s"$pathStr already considered")
      return false
    }
    logDebug(s"$pathStr accepted with mod time $modTime")
    return true
  }

  /** Generate one RDD from an array of files */
  private def filesToRDD(files: Seq[String]): RDD[(K, V)] = {
    val fileRDDs = files.map { file =>
      val rdd = serializableConfOpt.map(_.value) match {
        case Some(config) => context.sparkContext.newAPIHadoopFile(
          file,
          fm.runtimeClass.asInstanceOf[Class[F]],
          km.runtimeClass.asInstanceOf[Class[K]],
          vm.runtimeClass.asInstanceOf[Class[V]],
          config)
        case None => context.sparkContext.newAPIHadoopFile[K, V, F](file)
      }
      if (rdd.partitions.size == 0) {
        logError("File " + file + " has no data in it. Spark Streaming can only ingest " +
          "files that have been \"moved\" to the directory assigned to the file stream. " +
          "Refer to the streaming programming guide for more details.")
      }
      rdd
    }
    new UnionRDD(context.sparkContext, fileRDDs)
  }

  /** Get file mod time from cache or fetch it from the file system */
  private def getFileModTime(path: Path) = {
    fileToModTime.getOrElseUpdate(path.toString, fs.getFileStatus(path).getModificationTime())
  }

  private def directoryPath: Path = {
    if (path_ == null) path_ = new Path(directory)
    path_
  }

  private def fs: FileSystem = {
    if (fs_ == null) fs_ = directoryPath.getFileSystem(ssc.sparkContext.hadoopConfiguration)
    fs_
  }

  private def reset()  {
    fs_ = null
  }

  @throws(classOf[IOException])
  private def readObject(ois: ObjectInputStream): Unit = Utils.tryOrIOException {
    logDebug(this.getClass().getSimpleName + ".readObject used")
    ois.defaultReadObject()
    generatedRDDs = new mutable.HashMap[Time, RDD[(K, V)]]()
    batchTimeToSelectedFiles =
      new mutable.HashMap[Time, Array[String]] with mutable.SynchronizedMap[Time, Array[String]]
    recentlySelectedFiles = new mutable.HashSet[String]()
    fileToModTime = new TimeStampedHashMap[String, Long](true)
    
    batchTimeToNewFilesTime =
      new mutable.HashMap[Time, Time] with mutable.SynchronizedMap[Time, Time]
    lastNewFileFindedTime = null
  }

  /**
   * A custom version of the DStreamCheckpointData that stores names of
   * Hadoop files as checkpoint data.
   */
  private[streaming]
  class NonTimingUpdateFileInputDStreamCheckpointData extends DStreamCheckpointData(this) {

    private def hadoopFiles = data.asInstanceOf[mutable.HashMap[Time, Array[String]]]
    
    private val newFilesTime = new mutable.HashMap[Time, Time] with mutable.SynchronizedMap[Time, Time]
    
    override def update(time: Time) {
      hadoopFiles.clear()
      hadoopFiles ++= batchTimeToSelectedFiles
      
      newFilesTime.clear()
      newFilesTime ++= batchTimeToNewFilesTime
    }

    override def cleanup(time: Time) { }

    override def restore() {
      logDebug("hadoopFiles:\n" +
        hadoopFiles.map(p => (p._1, p._2.mkString(", "))).mkString("\n"))
      logDebug("newFilesTime:\n" +
        newFilesTime.map(p => (p._1, p._2)).mkString("\n"))

      batchTimeToNewFilesTime.clear()
      batchTimeToNewFilesTime ++= newFilesTime
      
      val timeToNewRDDs = batchTimeToNewFilesTime.map { (tt: (Time, Time)) => (tt._2, tt._1)}.map {
        case (newFilesTime, time) => {
          // Restore the metadata in both files and generatedRDDs
          val f = hadoopFiles.get(time).get
          logInfo("Restoring files for time " + newFilesTime + " - " +
            f.mkString("[", ", ", "]") )
          recentlySelectedFiles ++= f
          (newFilesTime, filesToRDD(f))
        }
      }
      
      hadoopFiles.toSeq.sortBy(_._1)(Time.ordering).foreach {
        case (t, f) => {
          batchTimeToSelectedFiles += ((t, f))
          generatedRDDs += ((t, timeToNewRDDs.get(batchTimeToNewFilesTime.get(t).get).get))
          lastNewFileFindedTime = batchTimeToNewFilesTime.get(t).get
        }
      }
      
      // logging
      logDebug("===============restore===========================")
      logDebug("batchTimeToSelectedFiles:\n" +
        batchTimeToSelectedFiles.map(p => (p._1, p._2.mkString(", "))).mkString("\n"))
      logDebug("recentlySelectedFiles:\n" + recentlySelectedFiles.mkString("\n"))
      logDebug("lastNewFileFindingTime:\n" + lastNewFileFindingTime.toString())
      logDebug("lastNewFileFindedTime:\n" + lastNewFileFindedTime.toString())
      logDebug("batchTimeToNewFilesTime:\n" +
        batchTimeToNewFilesTime.map(p => (p._1, p._2)).mkString("\n"))
      logDebug("===============restore===========================")
    }

    override def toString: String = {
      "[\n" + hadoopFiles.size + " file sets\n" +
        hadoopFiles.map(p => (p._1, p._2.mkString(", "))).mkString("\n") + "\n]"
    }
  }
}
