package org.apache.openwhisk.core.loadBalancer


import java.io.{FileWriter}

import akka.actor.{Actor, ActorRef, ActorRefFactory, ActorSystem, Props}
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.management.AkkaManagement
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.stream.ActorMaterializer
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.openwhisk.common._
import org.apache.openwhisk.core.WhiskConfig._
import org.apache.openwhisk.core.connector._
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.entity.size.SizeLong
import org.apache.openwhisk.core.{ConfigKeys, WhiskConfig}
import org.apache.openwhisk.spi.SpiLoader
import pureconfig._

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.io.Source

class ConfigurableLoadBalancer(config: WhiskConfig,
                               controllerInstance: ControllerInstanceId,
                               feedFactory: FeedFactory,
                               val invokerPoolFactory: InvokerPoolFactory,
                               implicit val messagingProvider: MessagingProvider = SpiLoader.get[MessagingProvider])(
                         implicit actorSystem: ActorSystem,
                         logging: Logging,
                         materializer: ActorMaterializer)
  extends CommonLoadBalancer(config, feedFactory, controllerInstance) {

  /** Build a cluster of all loadbalancers */
  private val cluster: Option[Cluster] = if (loadConfigOrThrow[ClusterConfig](ConfigKeys.cluster).useClusterBootstrap) {
    AkkaManagement(actorSystem).start()
    ClusterBootstrap(actorSystem).start()
    Some(Cluster(actorSystem))
  } else if (loadConfigOrThrow[Seq[String]]("akka.cluster.seed-nodes").nonEmpty) {
    Some(Cluster(actorSystem))
  } else {
    None
  }

  /**
   * Monitors invoker supervision and the cluster to update the state sequentially
   *
   * All state updates should go through this actor to guarantee that
   * [[ConfigurableLoadBalancerState.updateInvokers]] and [[ConfigurableLoadBalancerState.updateCluster]]
   * are called exclusive of each other and not concurrently.
   */
  private val monitor = actorSystem.actorOf(Props(new Actor {
    override def preStart(): Unit = {
      cluster.foreach(_.subscribe(self, classOf[MemberEvent], classOf[ReachabilityEvent]))
    }

    // all members of the cluster that are available
    var availableMembers = Set.empty[Member]

    override def receive: Receive = {
      case CurrentInvokerPoolState(newState) =>
        schedulingState.updateInvokers(newState)

      // State of the cluster as it is right now
      case CurrentClusterState(members, _, _, _, _) =>
        availableMembers = members.filter(_.status == MemberStatus.Up)
        schedulingState.updateCluster(availableMembers.size)

      // General lifecycle events and events concerning the reachability of members. Split-brain is not a huge concern
      // in this case as only the invoker-threshold is adjusted according to the perceived cluster-size.
      // Taking the unreachable member out of the cluster from that point-of-view results in a better experience
      // even under split-brain-conditions, as that (in the worst-case) results in premature overloading of invokers vs.
      // going into overflow mode prematurely.
      case event: ClusterDomainEvent =>
        availableMembers = event match {
          case MemberUp(member)          => availableMembers + member
          case ReachableMember(member)   => availableMembers + member
          case MemberRemoved(member, _)  => availableMembers - member
          case UnreachableMember(member) => availableMembers - member
          case _                         => availableMembers
        }

        schedulingState.updateCluster(availableMembers.size)
    }
  }))

  override protected val invokerPool: ActorRef =
    invokerPoolFactory.createInvokerPool(actorSystem, messagingProvider, messageProducer, sendActivationToInvoker, Some(monitor))

  val schedulingState: ConfigurableLoadBalancerState = ConfigurableLoadBalancerState()(loadConfigOrThrow[ConfigurableLoadBalancerConfig](ConfigKeys.loadbalancer))


  override protected def releaseInvoker(invoker: InvokerInstanceId, entry: ActivationEntry): Unit = {
    schedulingState.invokerSlots
      .lift(invoker.toInt)
      .foreach(_.releaseConcurrent(entry.fullyQualifiedEntityName, entry.maxConcurrent, entry.memoryLimit.toMB.toInt))
  }

  /**
   * Publishes activation message on internal bus for an invoker to pick up.
   *
   * @param action  the action to invoke
   * @param msg     the activation message to publish on an invoker topic
   * @param transid the transaction id for the request
   * @return result a nested Future the outer indicating completion of publishing and
   *         the inner the completion of the action (i.e., the result)
   *         if it is ready before timeout (Right) otherwise the activation id (Left).
   *         The future is guaranteed to complete within the declared action time limit
   *         plus a grace period (see activeAckTimeoutGrace).
   */
  override def publish(action: ExecutableWhiskActionMetaData, msg: ActivationMessage)
                      (implicit transid: TransactionId): Future[Future[Either[ActivationId, WhiskActivation]]] =
  {
    val isBlackboxInvocation = action.exec.pull
    val actionType = if (!isBlackboxInvocation) "managed" else "blackbox"
    val (invokersToUse, stepSizes) =
      if (!isBlackboxInvocation) (schedulingState.managedInvokers, schedulingState.managedStepSizes)
      else (schedulingState.blackboxInvokers, schedulingState.blackboxStepSizes)

    logging.info(this, "choosing invoker...")
    val f = new FileWriter("publish.txt", true)
    try {
      f.write("Choosing invoker: ")
    }
    catch {
      case e: Throwable => println(e)
    }
    finally {
      f.close()
    }
    val chosen = if (invokersToUse.nonEmpty) {
      val invoker: Option[(InvokerInstanceId, Boolean)] =
        ConfigurableLoadBalancer.schedule(action, msg,
          action.limits.concurrency.maxConcurrent,
          action.fullyQualifiedName(true),
          invokersToUse,
          schedulingState.invokerSlots,
          action.limits.memory.megabytes,
          action.annotations,
          stepSizes
        )(logging, transid)
      invoker.foreach {
        case (_, true) =>
          val metric =
            if (isBlackboxInvocation)
              LoggingMarkers.BLACKBOX_SYSTEM_OVERLOAD
            else
              LoggingMarkers.MANAGED_SYSTEM_OVERLOAD
          MetricEmitter.emitCounterMetric(metric)
        case _ =>
      }

      invoker.map(_._1)
    }
    else {
      logging.warn(this, "No invokers to use!")
      None
    }

    val ff = new FileWriter("publish.txt", true)

    try {
      ff.append(s"$chosen \n")
    }
    finally  {
      ff.close()
    }

    chosen.map {
      invoker =>
        // MemoryLimit() and TimeLimit() return singletons - they should be fast enough to be used here
        val memoryLimit = action.limits.memory
        val memoryLimitInfo = if (memoryLimit == MemoryLimit()) { "std" } else { "non-std" }
        val timeLimit = action.limits.timeout
        val timeLimitInfo = if (timeLimit == TimeLimit()) { "std" } else { "non-std" }
        logging.info(
          this,
          s"scheduled activation ${msg.activationId}, action '${msg.action.asString}' ($actionType), ns '${msg.user.namespace.name.asString}', mem limit ${memoryLimit.megabytes} MB ($memoryLimitInfo), time limit ${timeLimit.duration.toMillis} ms ($timeLimitInfo) to $invoker")
        val activationResult = setupActivation(msg, action, invoker)
        sendActivationToInvoker(messageProducer, msg, invoker).map(_ => activationResult)
      }
      .getOrElse {
        // report the state of all invokers
        val invokerStates = invokersToUse.foldLeft(Map.empty[InvokerState, Int]) { (agg, curr) =>
          val count = agg.getOrElse(curr.status, 0) + 1
          agg + (curr.status -> count)
        }

        logging.error(
          this,
          s"failed to schedule activation ${msg.activationId}, action '${msg.action.asString}' ($actionType), ns '${msg.user.namespace.name.asString}' - invokers to use: $invokerStates")
        Future.failed(LoadBalancerException("No invokers available"))
      }
  }

  /**
   * Returns a message indicating the health of the containers and/or container pool in general.
   *
   * @return a Future[IndexedSeq[InvokerHealth] representing the health of the pools managed by the loadbalancer.
   */
  override def invokerHealth(): Future[IndexedSeq[InvokerHealth]] = Future.successful(schedulingState.invokers)
}

object ConfigurableLoadBalancer extends LoadBalancerProvider {
  override def requiredProperties: Map[String, String] = kafkaHosts

  override def instance(whiskConfig: WhiskConfig, instance: ControllerInstanceId)
                       (implicit actorSystem: ActorSystem, logging: Logging, materializer: ActorMaterializer): LoadBalancer = {
    val invokerPoolFactory = new InvokerPoolFactory {
      override def createInvokerPool(
                                      actorRefFactory: ActorRefFactory,
                                      messagingProvider: MessagingProvider,
                                      messagingProducer: MessageProducer,
                                      sendActivationToInvoker: (MessageProducer, ActivationMessage, InvokerInstanceId) => Future[RecordMetadata],
                                      monitor: Option[ActorRef]): ActorRef = {

        InvokerPool.prepare(instance, WhiskEntityStore.datastore())

        actorRefFactory.actorOf(
          InvokerPool.props(
            (f, i) => f.actorOf(InvokerActor.props(i, instance)),
            (m, i) => sendActivationToInvoker(messagingProducer, m, i),
            messagingProvider.getConsumer(whiskConfig, s"health${instance.asString}", "health", maxPeek = 128),
            monitor))
      }

    }

    new ConfigurableLoadBalancer(
      whiskConfig,
      instance,
      createFeedFactory(whiskConfig, instance),
      invokerPoolFactory)
  }

  val configurationText : String = loadConfigText("configLB.yml")

  var configurableLBSettings : ConfigurableLBSettings = LBControlParser.parseConfigurableLBSettings(configurationText)

  def getTag(annotations: Parameters)(implicit logging: Logging, transId: TransactionId): Option[String] = {
    annotations.get("parameters") match {
      case Some(json) =>
        logging.info(this, s"Converting $json into Tag object...")
        logIntoContainer(s"ConfigurableLB: Converting $json into Tag object...")
        import spray.json.DefaultJsonProtocol._
        val t: List[Map[String, String]] = json.convertTo[List[Map[String,String]]]
        t.find(m => m.contains("tag")) match {
          case Some(m) =>
            val tag = m("tag")
            logging.info(this, s"Received: $tag")
            logIntoContainer(s"ConfigurableLB: Received: $tag")
            Some(tag)
          case None => None
        }
      case None =>
        logging.info(this, "No tag received")
        logIntoContainer("ConfigurableLB: No tag received")
        None
    }
  }

  def defaultSchedule(action: ExecutableWhiskActionMetaData, msg: ActivationMessage, invokers: IndexedSeq[InvokerHealth],
                       dispatched: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]], stepSizes : Seq[Int],
                      maxC : Option[Int], maxF : Option[Int])
                     (implicit logging: Logging, transId: TransactionId): Option[(InvokerInstanceId, Boolean)] =
  {
    val hash = ShardingContainerPoolBalancer.generateHash(msg.user.namespace.name, action.fullyQualifiedName(false))
    val homeInvoker = hash % invokers.size
    val stepSize = stepSizes(hash % stepSizes.size)
    val invoker = ShardingContainerPoolBalancer.schedule(
      maxF.getOrElse(action.limits.concurrency.maxConcurrent),
      action.fullyQualifiedName(true),
      invokers,
      dispatched,
      maxC.getOrElse(action.limits.memory.megabytes),
      homeInvoker,
      stepSize)
    logging.info(this, "Invoker chosen with default scheduler!")
    logIntoContainer("Invoker chosen with default scheduler!")
    invoker
  }

  def loadConfigText(fileName: String) : String =
    {
      logIntoContainer("ConfigurableLB: Loading configuration from " + fileName)
      val source = Source.fromFile(fileName)
      val text = try source.mkString finally source.close()
      logIntoContainer("ConfigurableLB: Config text " + text)
      text
    }

  def logIntoContainer(msg: String) = {
    val fw = new FileWriter("logs.txt", true)
    try {
      fw.append(s"$msg\n")
      }
    catch {
      case e: Throwable => println(e)
    }
    finally {
      fw.close()
    }
  }

  def schedule(
                action: ExecutableWhiskActionMetaData, msg: ActivationMessage,
                maxConcurrent: Int,
                fqn: FullyQualifiedEntityName,
                invokers: IndexedSeq[InvokerHealth],
                dispatched: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]],
                slots: Int,
                annotations: Parameters,
                stepSizes: Seq[Int]
              )(implicit logging: Logging, transId: TransactionId): Option[(InvokerInstanceId, Boolean)] =
  {

    @scala.annotation.tailrec
    def bestFirstSchedule(specifiedInvokers:  List[InvokerHealth], maxC : Option[Int], maxF : Option[Int]): Option[(InvokerInstanceId, Boolean)] =
    specifiedInvokers match {
      case IndexedSeq() => None
      case (x : InvokerHealth) :: xs =>
        if (dispatched(x.id.toInt).tryAcquireConcurrent(fqn, maxF.getOrElse(maxConcurrent), maxC.getOrElse(slots))) Some(x.id, false)
        else bestFirstSchedule(xs, maxC, maxF)
    }

    @scala.annotation.tailrec
    def randomSchedule(specifiedInvokers:  IndexedSeq[InvokerHealth], maxC : Option[Int], maxF : Option[Int]): Option[(InvokerInstanceId, Boolean)] = {

      def getRandomIndex = {
        val rand = scala.util.Random
        val rIndex = rand.nextInt(specifiedInvokers.length)
        rIndex
      }
      val rIndex = getRandomIndex
      val x = specifiedInvokers(rIndex)
      logIntoContainer(s"ConfigurableLB: Random scheduling chosen... number of invokers: ${specifiedInvokers.length} index chosen: $rIndex")
      if (dispatched(x.id.toInt).tryAcquireConcurrent(fqn, maxF.getOrElse(maxConcurrent), maxC.getOrElse(slots))) Some(x.id, false)
      else randomSchedule(specifiedInvokers.take(rIndex-1) ++ specifiedInvokers.drop(rIndex), maxC, maxF )

    }
    def scheduleByStrategy(specifiedInvokers: IndexedSeq[InvokerHealth], strategy: Option[String],
                           maxC : Option[Int], maxF : Option[Int]): Option[(InvokerInstanceId, Boolean)] =
      strategy match {
        case None =>  defaultSchedule(action, msg, specifiedInvokers, dispatched, stepSizes, maxC, maxF)
        case Some("best-first") => bestFirstSchedule(specifiedInvokers.toList, maxC, maxF)
        case Some("random") => randomSchedule(specifiedInvokers, maxC, maxF)
        case Some("next-coprime") =>  defaultSchedule(action, msg, specifiedInvokers, dispatched, stepSizes, maxC, maxF)
        case _ => throw new Exception("No strategy found")
        // if the specified invokers are available, check for max capacity and max conc invocations
        /*
        val (azureInvokers, otherInvokers) = healthyInvokers.partition(_.id.uniqueName.getOrElse("") == tag)

        logging.info(this, "Partitioned invoker list in :" + azureInvokers + " and " + otherInvokers)
        if (azureInvokers.nonEmpty) {
          logging.info(this, "From azureInvokers: " + azureInvokers(0).id.uniqueName.getOrElse("no unique name"))
          logIntoContainer("From azureInvokers: " + azureInvokers(0).id.uniqueName.getOrElse("no unique name"))

          val availableAzureInvokers = azureInvokers.filter(i => dispatched(i.id.toInt).tryAcquireConcurrent(fqn, maxConcurrent, slots)) // invokers with spare capacity
          logging.info(this, "Chosen azure invoker: " + availableAzureInvokers(0).id + " on " + availableAzureInvokers(0).id.uniqueName)
          if (availableAzureInvokers.nonEmpty) Some(availableAzureInvokers(0).id, false)
          else getOtherInvoker(availableAzureInvokers(0).id, maxConcurrent, fqn, otherInvokers, dispatched, slots) // azure invokers have no capacity, fall back on otherInvokers
        }
        else getOtherInvoker(null, maxConcurrent, fqn, otherInvokers, dispatched, slots)*/


      }

    @scala.annotation.tailrec
    def scheduleBasedOnTagSettings(healthyInvokers: IndexedSeq[InvokerHealth],
                                   settingsList: List[InvokersSetSettings],
                                   followUp : Option[String],
                                   defaultFallBack: Option[TagSettings]): Option[(InvokerInstanceId, Boolean)] =
      settingsList match {
        // if no invokers settings worked, default schedule
        case List() => 
          followUp match {
            case None => {
              defaultFallBack match {
                case None => defaultSchedule(action, msg, invokers, dispatched, stepSizes, None, None)
                case Some(ts) => scheduleBasedOnTagSettings(healthyInvokers, ts.invokersSettings, ts.followUp, None)
              }
            }
            case Some(s) if (s == "fail") => 
              logIntoContainer(s"No compatible invokers found and followup is fail. Can't invoke action!")
              None
          }
        case x :: xs =>
          logIntoContainer(s"ConfigurableLB scheduleBasedOnTagSettings number of settings ${settingsList.length} and defaultFallback: ${defaultFallBack.isDefined}")
          val res = scheduleOnSpecifiedInvokersSet(healthyInvokers, x)
          if (res.isDefined) {
            logIntoContainer(s"ConfigurableLB scheduleBasedOnTagSettings invoker found! ${res}")
            res
          }
          else scheduleBasedOnTagSettings(healthyInvokers, xs, followUp, defaultFallBack)
    }

    def scheduleOnSpecifiedInvokersSet(healthyInvokers: IndexedSeq[InvokerHealth], 
                                        setSettings : InvokersSetSettings) : Option[(InvokerInstanceId, Boolean)] =
    {
      logIntoContainer("ConfigurableLB: scheduleOnSpecifiedInvokersSet with settings: " + setSettings + " and healthy invokers: "+ healthyInvokers.map(i => i.id))

      setSettings.workers match {
        case InvokerList(names) => 
        {
          val cInvokers = healthyInvokers.filter(i => names.contains(i.id.uniqueName.getOrElse("")))
          if (cInvokers.isEmpty) None
          else {
            scheduleByStrategy(cInvokers, setSettings.strategy, setSettings.maxCapacity, setSettings.maxConcurrentInvocations)
          }   
        }
        case All() => scheduleByStrategy(healthyInvokers, setSettings.strategy, setSettings.maxCapacity, setSettings.maxConcurrentInvocations)
      }
    }

    val tag : String = getTag(annotations).getOrElse("default") // no tag received, switch to default

    val tagSettings: Option[TagSettings] = configurableLBSettings.getTagSettings(tag)
    
    if (invokers.size > 0) {
      val healthyInvokers: IndexedSeq[InvokerHealth] = invokers.filter(_.status.isUsable) // get only healthy invokers

      logging.info(this, s"ConfigurableLB: tag = $tag with settings $tagSettings and invokers: $invokers")
      logIntoContainer(s"ConfigurableLB: tag = $tag with settings $tagSettings and healthy invokers: $healthyInvokers")
    
      tagSettings match {
        case None =>
          logIntoContainer("ConfigurableLB: invocation without default and tag, fall back to default invocation.")
          defaultSchedule(action, msg, invokers, dispatched, stepSizes, None, None)
        case Some(ts) =>
          logIntoContainer(s"ConfigurableLB tagSettings = ${tagSettings}")
          val defaultSettings: Option[TagSettings] = configurableLBSettings.getTagSettings("default")
          scheduleBasedOnTagSettings(healthyInvokers, ts.invokersSettings, ts.followUp, defaultSettings)
      }
    } else {
      logging.info(this, "Number of invokers is 0")
      logIntoContainer("ConfigurableLB: Cant' invoker action, number of invokers is 0.")
      None
    }
  }

  def getOtherInvoker(chosenInvoker: InvokerInstanceId,
                      maxConcurrent: Int,
                      fqn: FullyQualifiedEntityName,
                      otherInvokers: IndexedSeq[InvokerHealth],
                      dispatched: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]],
                      slots: Int)(implicit logging: Logging, transId: TransactionId) : Option[(InvokerInstanceId, Boolean)] = {
    if (otherInvokers.nonEmpty) {
      val invokersWithCapacity = otherInvokers.filter(i => dispatched(i.id.toInt).tryAcquireConcurrent(fqn, maxConcurrent, slots)) // filter by capacity

      if (invokersWithCapacity.nonEmpty) {
        logging.info(this, "From other invokers: " + invokersWithCapacity(0).id.uniqueName.getOrElse("no unique name"))
        logIntoContainer("From other invokers: " + invokersWithCapacity(0).id.uniqueName.getOrElse("no unique name"))
        Some(invokersWithCapacity(0).id, false)
      }
      else if (chosenInvoker != null) Some(chosenInvoker, true) // other invokers have no capacity either
      else Some(otherInvokers(0).id, true)
    }
    else Some(chosenInvoker, true)
  }

}


case class ConfigurableLoadBalancerState(
                                   private var _invokers: IndexedSeq[InvokerHealth] = IndexedSeq.empty[InvokerHealth],
                                   private var _managedInvokers: IndexedSeq[InvokerHealth] = IndexedSeq.empty[InvokerHealth],
                                   private var _blackboxInvokers: IndexedSeq[InvokerHealth] = IndexedSeq.empty[InvokerHealth],
                                   private var _managedStepSizes: Seq[Int] = ShardingContainerPoolBalancer.pairwiseCoprimeNumbersUntil(0),
                                   private var _blackboxStepSizes: Seq[Int] = ShardingContainerPoolBalancer.pairwiseCoprimeNumbersUntil(0),
                                   protected[loadBalancer] var _invokerSlots: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]] =
                                   IndexedSeq.empty[NestedSemaphore[FullyQualifiedEntityName]],
                                   private var _clusterSize: Int = 1)(
                                   lbConfig: ConfigurableLoadBalancerConfig =
                                   loadConfigOrThrow[ConfigurableLoadBalancerConfig](ConfigKeys.loadbalancer))(implicit logging: Logging)
{

  // Managed fraction and blackbox fraction can be between 0.0 and 1.0. The sum of these two fractions has to be between
  // 1.0 and 2.0.
  // If the sum is 1.0 that means, that there is no overlap of blackbox and managed invokers. If the sum is 2.0, that
  // means, that there is no differentiation between managed and blackbox invokers.
  // If the sum is below 1.0 with the initial values from config, the blackbox fraction will be set higher than
  // specified in config and adapted to the managed fraction.
  private val managedFraction: Double = Math.max(0.0, Math.min(1.0, lbConfig.managedFraction))
  private val blackboxFraction: Double = Math.max(1.0 - managedFraction, Math.min(1.0, lbConfig.blackboxFraction))
  logging.info(this, s"ConfigurableLB: managedFraction = $managedFraction, blackboxFraction = $blackboxFraction")(
    TransactionId.loadbalancer)

  /** Getters for the variables, setting from the outside is only allowed through the update methods below */
  def invokers: IndexedSeq[InvokerHealth] = _invokers
  def managedInvokers: IndexedSeq[InvokerHealth] = _managedInvokers
  def blackboxInvokers: IndexedSeq[InvokerHealth] = _blackboxInvokers
  def blackboxStepSizes: Seq[Int] = _blackboxStepSizes
  def managedStepSizes: Seq[Int] = _managedStepSizes

  def invokerSlots: IndexedSeq[NestedSemaphore[FullyQualifiedEntityName]] = _invokerSlots
  def clusterSize: Int = _clusterSize


  private def getInvokerSlot(memory: ByteSize): ByteSize = {
    val invokerShardMemorySize = memory / _clusterSize
    val newThreshold = if (invokerShardMemorySize < MemoryLimit.MIN_MEMORY) {
      logging.error(
        this,
        s"registered controllers: calculated controller's invoker shard memory size falls below the min memory of one action. "
          + s"Setting to min memory. Expect invoker overloads. Cluster size ${_clusterSize}, invoker user memory size ${memory.toMB.MB}, "
          + s"min action memory size ${MemoryLimit.MIN_MEMORY.toMB.MB}, calculated shard size ${invokerShardMemorySize.toMB.MB}.")(
        TransactionId.loadbalancer)
      MemoryLimit.MIN_MEMORY
    } else {
      invokerShardMemorySize
    }
    newThreshold
  }

  /**
   * Updates the scheduling state with the new invokers.
   *
   * This is okay to not happen atomically since dirty reads of the values set are not dangerous. It is important though
   * to update the "invokers" variables last, since they will determine the range of invokers to choose from.
   *
   * Handling a shrinking invokers list is not necessary, because InvokerPool won't shrink its own list but rather
   * report the invoker as "Offline".
   *
   * It is important that this method does not run concurrently to itself and/or to [[updateCluster]]
   */
  def updateInvokers(newInvokers: IndexedSeq[InvokerHealth]): Unit = {
    val oldSize = _invokers.size
    val newSize = newInvokers.size

    // for small N, allow the managed invokers to overlap with blackbox invokers, and
    // further assume that blackbox invokers << managed invokers
    val managed = Math.max(1, Math.ceil(newSize.toDouble * managedFraction).toInt)
    val blackboxes = Math.max(1, Math.floor(newSize.toDouble * blackboxFraction).toInt)

    _invokers = newInvokers
    _managedInvokers = _invokers.take(managed)
    _blackboxInvokers = _invokers.takeRight(blackboxes)

    val logDetail = if (oldSize != newSize) {

      _managedStepSizes = ShardingContainerPoolBalancer.pairwiseCoprimeNumbersUntil(managed)
      _blackboxStepSizes = ShardingContainerPoolBalancer.pairwiseCoprimeNumbersUntil(blackboxes)
      if (oldSize < newSize) {
        // Keeps the existing state..
        val onlyNewInvokers = _invokers.drop(_invokerSlots.length)
        _invokerSlots = _invokerSlots ++ onlyNewInvokers.map { invoker =>
          new NestedSemaphore[FullyQualifiedEntityName](getInvokerSlot(invoker.id.userMemory).toMB.toInt)
        }
        val newInvokerDetails = onlyNewInvokers
          .map(i =>
            s"${i.id.toString}: ${i.status} / ${getInvokerSlot(i.id.userMemory).toMB.MB} of ${i.id.userMemory.toMB.MB}")
          .mkString(", ")
        s"number of known invokers increased: new = $newSize, old = $oldSize. details: $newInvokerDetails."
      } else {
        s"number of known invokers decreased: new = $newSize, old = $oldSize."
      }
    } else {
      s"no update required - number of known invokers unchanged: $newSize."
    }

    logging.info(
      this,
      s"loadbalancer invoker status updated. managedInvokers = $managed blackboxInvokers = $blackboxes. $logDetail")(
      TransactionId.loadbalancer)
  }

  /**
   * Updates the size of a cluster. Throws away all state for simplicity.
   *
   * This is okay to not happen atomically, since a dirty read of the values set are not dangerous. At worst the
   * scheduler works on outdated invoker-load data which is acceptable.
   *
   * It is important that this method does not run concurrently to itself and/or to [[updateInvokers]]
   */
  def updateCluster(newSize: Int): Unit = {
    val actualSize = newSize max 1 // if a cluster size < 1 is reported, falls back to a size of 1 (alone)
    if (_clusterSize != actualSize) {
      val oldSize = _clusterSize
      _clusterSize = actualSize
      _invokerSlots = _invokers.map { invoker =>
        new NestedSemaphore[FullyQualifiedEntityName](getInvokerSlot(invoker.id.userMemory).toMB.toInt)
      }
      // Directly after startup, no invokers have registered yet. This needs to be handled gracefully.
      val invokerCount = _invokers.size
      val totalInvokerMemory =
        _invokers.foldLeft(0L)((total, invoker) => total + getInvokerSlot(invoker.id.userMemory).toMB).MB
      val averageInvokerMemory =
        if (totalInvokerMemory.toMB > 0 && invokerCount > 0) {
          (totalInvokerMemory / invokerCount).toMB.MB
        } else {
          0.MB
        }
      logging.info(
        this,
        s"loadbalancer cluster size changed from $oldSize to $actualSize active nodes. $invokerCount invokers with $averageInvokerMemory average memory size - total invoker memory $totalInvokerMemory.")(
        TransactionId.loadbalancer)
    }
  }

}

/**
 * Configuration for the pool balancer.
 *
 * @param blackboxFraction the fraction of all invokers to use exclusively for blackboxes
 * @param timeoutFactor factor to influence the timeout period for forced active acks (time-limit.std * timeoutFactor + timeoutAddon)
 * @param timeoutAddon extra time to influence the timeout period for forced active acks (time-limit.std * timeoutFactor + timeoutAddon)
 */
case class ConfigurableLoadBalancerConfig(
                                    managedFraction: Double,
                                    blackboxFraction: Double,
                                    timeoutFactor: Int,
                                    timeoutAddon: FiniteDuration
                                  )
