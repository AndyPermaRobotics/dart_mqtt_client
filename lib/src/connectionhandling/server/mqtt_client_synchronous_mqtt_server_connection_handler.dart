/*
 * Package : mqtt_client
 * Author : S. Hamblett <steve.hamblett@linux.com>
 * Date   : 22/06/2017
 * Copyright :  S.Hamblett
 */

part of '../../../mqtt_server_client.dart';

/// Connection handler that performs server based connections and disconnections
/// to the hostname in a synchronous manner.
class SynchronousMqttServerConnectionHandler extends MqttServerConnectionHandler {
  /// Initializes a new instance of the SynchronousMqttConnectionHandler class.
  SynchronousMqttServerConnectionHandler(super.clientEventBus, {required int maxConnectionAttempts, required super.socketOptions, reconnectTimePeriod = 5000})
      : super(maxConnectionAttempts: maxConnectionAttempts) {
    connectTimer = MqttCancellableAsyncSleep(reconnectTimePeriod);
    initialiseListeners();
  }

  /// Synchronously connect to the specific Mqtt Connection.
  @override
  Future<MqttClientConnectionStatus> internalConnect(String hostname, int port, MqttConnectMessage? connectMessage) async {
    var connectionAttempts = 0;
    MqttLogger.log('SynchronousMqttServerConnectionHandler::internalConnect entered');
    do {
      print("ANDY: Connection Attempt: $connectionAttempts");

      // Initiate the connection
      MqttLogger.log('SynchronousMqttServerConnectionHandler::internalConnect - '
          'initiating connection try $connectionAttempts, auto reconnect in progress $autoReconnectInProgress');
      connectionStatus.state = MqttConnectionState.connecting;
      connectionStatus.returnCode = MqttConnectReturnCode.noneSpecified;
      // Don't reallocate the connection if this is an auto reconnect
      if (!autoReconnectInProgress) {
        if (useWebSocket) {
          final MqttServerWsConnection connection;
          if (useAlternateWebSocketImplementation) {
            MqttLogger.log('SynchronousMqttServerConnectionHandler::internalConnect - '
                'alternate websocket implementation selected');
            connection = MqttServerWs2Connection(securityContext, clientEventBus, socketOptions);
          } else {
            MqttLogger.log('SynchronousMqttServerConnectionHandler::internalConnect - '
                'websocket selected');
            connection = MqttServerWsConnection(clientEventBus, socketOptions);
          }

          final websocketProtocols = this.websocketProtocols;
          if (websocketProtocols != null) {
            connection.protocols = websocketProtocols;
          }

          this.connection = connection;
        } else if (secure) {
          MqttLogger.log('SynchronousMqttServerConnectionHandler::internalConnect - '
              'secure selected');
          connection = MqttServerSecureConnection(securityContext, clientEventBus, onBadCertificate, socketOptions);
        } else {
          MqttLogger.log('SynchronousMqttServerConnectionHandler::internalConnect - '
              'insecure TCP selected');
          connection = MqttServerNormalConnection(clientEventBus, socketOptions);
        }
        connection.onDisconnected = onDisconnected;
      }

      Completer<void> completer = Completer<void>();

      // Connect
      //We run this in a zone, to catch uncaught SocketExceptions
      unawaited(
        runZonedGuarded(
          () async {
            try {
              if (!autoReconnectInProgress) {
                MqttLogger.log('SynchronousMqttServerConnectionHandler::internalConnect - calling connect');
                await connection.connect(hostname, port);
              } else {
                MqttLogger.log('SynchronousMqttServerConnectionHandler::internalConnect - calling connectAuto');
                await connection.connectAuto(hostname, port);
              }

              MqttLogger.log('SynchronousMqttServerConnectionHandler::internalConnect - '
                  'connection complete');
              // Transmit the required connection message to the broker.
              MqttLogger.log('SynchronousMqttServerConnectionHandler::internalConnect '
                  'sending connect message');
              sendMessage(connectMessage);
              MqttLogger.log('SynchronousMqttServerConnectionHandler::internalConnect - '
                  'pre sleep, state = $connectionStatus');
              // We're the sync connection handler so we need to wait for the
              // brokers acknowledgement of the connections
              await connectTimer.sleep();
              MqttLogger.log('SynchronousMqttServerConnectionHandler::internalConnect - '
                  'post sleep, state = $connectionStatus');

              completer.complete();
            } catch (e) {
              //print("ANDY Inner catch(e): $e");

              if (completer.isCompleted) {
                //print("ANDY: Already completed -> skip complete");
                return;
              }
              completer.complete();
            }
          },
          (error, stack) {
            //   print("ANDY: Uncaught Error: $error, $stack");

            //   MqttLogger.log('SynchronousMqttServerConnectionHandler::internalConnect - '
            //       'Uncaught Exception in internalConnect(): $error, autoReconnectInProgress: $autoReconnectInProgress');

            //   // Note: if we throw an error here, it won't be caught by the outer try/catch block

            //print("ANDY Complete runZonedGuarded (error)");

            if (completer.isCompleted) {
              //print("ANDY: Already completed -> skip complete");

              return;
            }

            completer.complete();
          },
        ),
      );

      //print("ANDY - Waiting for completer");

      await completer.future;

      //print("ANDY: End of loop");
    } while (connectionStatus.state != MqttConnectionState.connected && ++connectionAttempts < maxConnectionAttempts!);
    // If we've failed to handshake with the broker, throw an exception.
    if (connectionStatus.state != MqttConnectionState.connected) {
      if (!autoReconnectInProgress) {
        MqttLogger.log('SynchronousMqttServerConnectionHandler::internalConnect failed');
        if (connectionStatus.returnCode == MqttConnectReturnCode.noneSpecified) {
          throw NoConnectionException('The maximum allowed connection attempts '
              '({$maxConnectionAttempts}) were exceeded. '
              'The broker is not responding to the connection request message '
              '(Missing Connection Acknowledgement?');
        } else {
          throw NoConnectionException('The maximum allowed connection attempts '
              '({$maxConnectionAttempts}) were exceeded. '
              'The broker is not responding to the connection request message correctly '
              'The return code is ${connectionStatus.returnCode}');
        }
      }
    }
    MqttLogger.log('SynchronousMqttServerConnectionHandler::internalConnect '
        'exited with state $connectionStatus');
    initialConnectionComplete = true;
    return connectionStatus;
  }
}
