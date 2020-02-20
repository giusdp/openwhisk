package org.apache.openwhisk.core.loadBalancer

import java.util
import java.io.{FileWriter}

import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConverters._

sealed trait Invokers
case class InvokerList(names: List[String]) extends Invokers
case class All() extends Invokers

// Oggetto per ogni insieme di workers che contiene gli workers
// strategia associata e invalidate (capacity e max concs)
case class InvokersSetSettings(
  workers: Invokers, strategy: Option[String],
  maxCapacity: Option[Int], 
  maxConcurrentInvocations: Option[Int]) {
    override def toString: String = s"${workers}, strat: ${strategy}, maxC ${maxCapacity}, maxCI ${maxConcurrentInvocations}"
}

// Oggetto per ogni function tag che contiene tutte le impostazioni
case class TagSettings(tag: String, invokersSettings: List[InvokersSetSettings], followUp : Option[String])
{
  override def toString: String = s"Tag Settings for tag: $tag. With ${invokersSettings.length} workers and ${followUp} followup. ${invokersSettings}"
}

// Oggetto per tutte le impostazioni di un file smart lb config yml
case class ConfigurableLBSettings(settings : Map[String, TagSettings]) {
  // Dovrebbe avere una mappa tag -> invokerList
  // Si perchè al momento andrebbe a fare una roba tipo tagsettings filter tag. Invece è meglio avere tag -> tagsettings

  def getTagSettings(tag : String): Option[TagSettings] = settings.get(tag)

}

object LBControlParser {

  def logIntoContainer(msg: String) = {
    val fw = new FileWriter("parserLogs.txt", true)
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

  private def parseInvokersSettings(invokerSettings: Map[String, Any]) : InvokersSetSettings = {

    logIntoContainer(s"${invokerSettings("workers")}")
    val invokersList = invokerSettings("workers") match {
      case "*" => All()
      case l => InvokerList(l.asInstanceOf[util.ArrayList[String]].asScala.toList)
    }
    logIntoContainer(s"$invokersList")

    val strategy = invokerSettings.get("strategy").asInstanceOf[Option[String]]
    
    logIntoContainer(s"$strategy")

    val invalidate: Map[String, Int] = invokerSettings.get("invalidate") match {
      case Some("overload") => Map()
      case l => l.asInstanceOf[Option[util.ArrayList[util.HashMap[String, Int]]]] match {
        case None => Map()
        case Some(l) => l.asScala.toList.flatMap(m => m.asScala.toMap).toMap
      }
    }

    logIntoContainer(s"$invalidate")

    InvokersSetSettings(invokersList, strategy, invalidate.get("capacity_used"), invalidate.get("max_concurrent_invocations"))
  }

  private def parseTagSettings(tag : (String, Any)) : (String, TagSettings) = {
    val tagName = tag._1
    
    val settings: List[Map[String, Any]] = tag._2.asInstanceOf[util.ArrayList[util.HashMap[String, Any]]].asScala.toList.map(_.asScala.toMap)
    
    val (invokersSettings: List[Map[String, Any]], followupList: List[Map[String, Any]]) = settings.partition(!_.contains("followup"))

    val fl = followupList.map(m=>m.get("followup"))
    
    val followUp: Option[String] = fl match {
      case List() => None
      case x :: _ => if (x.isDefined) Some(x.get.toString) else None
    }
    tagName -> TagSettings(tagName, invokersSettings.map(parseInvokersSettings), followUp)
  }

  def parseConfigurableLBSettings(configurationYAMLText : String) : ConfigurableLBSettings = {
    val parsedYaml: Map[String, Any] = new Yaml().load[util.HashMap[String, Any]](configurationYAMLText).asScala.toMap

    logIntoContainer(parsedYaml.toString)

    ConfigurableLBSettings(parsedYaml.map(parseTagSettings))
  }

}
