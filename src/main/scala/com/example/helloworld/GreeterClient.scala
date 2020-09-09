package com.example.helloworld

//#import
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import akka.Done
import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.{ActorSystem => ClassicActorSystem}
import akka.grpc.GrpcClientSettings
import akka.pattern.CircuitBreaker
import akka.stream.scaladsl.Source
import io.grpc.CallOptions
import io.grpc.Channel
import io.grpc.ClientCall
import io.grpc.ClientInterceptor
import io.grpc.ClientInterceptors
import io.grpc.ForwardingClientCallListener
import io.grpc.Metadata
import io.grpc.MethodDescriptor
import io.grpc.Status


//#import

//#client-request-reply
object GreeterClient {

  def main(args: Array[String]): Unit = {
    implicit val sys: ActorSystem[_] = ActorSystem(Behaviors.empty, "GreeterClient")
    val classic:  ClassicActorSystem = sys.classicSystem
    implicit val ec: ExecutionContext = sys.executionContext

    val breaker =
      CircuitBreaker(classic.scheduler, maxFailures = 5, callTimeout = 3.seconds, resetTimeout = 10.seconds)

    // implement an interceptor for circuit breaking (https://github.com/grpc/grpc-java/issues/7281#issuecomment-671429104)
    // and the impl at https://gist.github.com/eungju/226274b3dacb3203de3514bcf1c54505#file-circuitbreakerclientinterceptor-kt


    class Listener[RespT](delegate: ClientCall.Listener[RespT]) extends  ForwardingClientCallListener.SimpleForwardingClientCallListener[RespT](delegate) {
      override def onClose(status: Status, trailers: Metadata): Unit = {
        if(status.isOk){
          breaker.succeed()
        } else {
          // TODO: capture the cause
          //          cause = new StatusRuntimeException(status, trailers)
          breaker.fail()
        }

        super.onClose(status, trailers)
      }
    }

    val circuitBreakingInterceptor: ClientInterceptor = new ClientInterceptor {
      override def interceptCall[ReqT, RespT](method: MethodDescriptor[ReqT, RespT], callOptions: CallOptions, next: Channel): ClientCall[ReqT, RespT] =
                new ClientInterceptors.CheckedForwardingClientCall[ReqT, RespT](next.newCall(method, callOptions)) {

                  override def checkedStart(responseListener: ClientCall.Listener[RespT], headers: Metadata): Unit = {
                      delegate().start(new Listener(responseListener), headers)
                  }

                  override def isReady: Boolean = {
                    if (breaker.isClosed || breaker.isHalfOpen) {
                      super.isReady
                    } else {
                      throw new IllegalStateException("Circuit Breaker is open. Can't send messages.")
                    }
                  }

                  override def sendMessage(message: ReqT): Unit = {
                    if (breaker.isClosed || breaker.isHalfOpen) {
                      delegate().sendMessage(message)
                    }
                  }
                }
    }

    val clientSettings = GrpcClientSettings.fromConfig("helloworld.GreeterService")
      .withChannelBuilderOverrides{ builder =>
        builder.intercept(circuitBreakingInterceptor)
      }

    val client = GreeterServiceClient(clientSettings)

    val names =
      if (args.isEmpty) List("Alice", "Bob", "Caroline", "Daniel", "Esperanza")
      else args.toList

    Source
      .fromIterator(() => Iterator.from(1))
      .flatMapConcat( i => Source.fromIterator(() => names.iterator).map( name => s"$i - $name"))
      .throttle(1, 1.second)
      .runForeach(id => Try(singleRequestReply(id)))

    //#client-request-reply
    if (args.nonEmpty)
      names.foreach(streamingBroadcast)
    //#client-request-reply

    def singleRequestReply(name: String): Unit = {
      println(s"Performing request: $name")
      val reply = client.sayHello(HelloRequest(name))
      reply.onComplete {
        case Success(msg) =>
          println(msg)
        case Failure(e) =>
          println(s"Error: $e")
      }
    }

    //#client-request-reply
    //#client-stream
    def streamingBroadcast(name: String): Unit = {
      println(s"Performing streaming requests: $name")

      val requestStream: Source[HelloRequest, NotUsed] =
        Source
          .tick(1.second, 1.second, "tick")
          .zipWithIndex
          .map { case (_, i) => i }
          .map(i => HelloRequest(s"$name-$i"))
          .mapMaterializedValue(_ => NotUsed)

      val responseStream: Source[HelloReply, NotUsed] = client.sayHelloToAll(requestStream)
      val done: Future[Done] =
        responseStream.runForeach(reply => println(s"$name got streaming reply: ${reply.message}"))

      done.onComplete {
        case Success(_) =>
          println("streamingBroadcast done")
        case Failure(e) =>
          println(s"Error streamingBroadcast: $e")
      }
    }
    //#client-stream
    //#client-request-reply

  }

}
//#client-request-reply
