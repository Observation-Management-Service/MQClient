"""Common logging strings."""

SENDING_MESSAGE = "[send_message()] Sending message..."
SENT_MESSAGE = "[send_message()] Sent message."

GETMSG_RECEIVE_MESSAGE = "[get_message()] Trying to receive message..."
GETMSG_RECEIVED_MESSAGE = "[get_message()] Received message."
GETMSG_NO_MESSAGE = "[get_message()] Didn't receive message. Returning None."
GETMSG_TIMEOUT_ERROR = "[get_message()] Timeout error. Returning None."
GETMSG_CONNECTION_ERROR_TRY_AGAIN = "[get_message()] Connection error. Trying again."
GETMSG_RAISE_OTHER_ERROR = "[get_message()] Other error. Raising Exception."
GETMSG_CONNECTION_ERROR_MAX_RETRIES = "[get_message()] Connection error. Reached max retries. Raising Exception."

ACKING_MESSAGE = "[ack_message()] Ack'ing message..."
ACKED_MESSAGE = "[ack_message()] Ack'd message."

NACKING_MESSAGE = "[reject_message()] Nack'ing message..."
NACKED_MESSAGE = "[reject_message()] Nack'd message."

MSGGEN_GET_NEW_MESSAGE = "[message_generator()] Getting a new message..."
MSGGEN_NO_MESSAGE_LOOK_BACK_IN_QUEUE = "[message_generator()] No messages in idle timeout window."
MSGGEN_YIELDING_MESSAGE = "[message_generator()] Yielding message..."
MSGGEN_DOWNSTREAM_ERROR = "[message_generator()] There was a downstream error."
MSGGEN_PROPAGATING_ERROR = "[message_generator()] Propagating error..."
MSGGEN_EXCEPTED_DOWNSTREAM_ERROR = "[message_generator()] Excepted downstream error (not re-raising):"
MSGGEN_GENERATOR_EXIT = "[message_generator()] GeneratorExit."
MSGGEN_CLOSED_QUEUE = "[message_generator()] Closed queue."

TRYCALL_CONNECTION_CLOSED_BY_BROKER = "[try_call()] ConnectionClosedByBroker..."
TRYCALL_AMQP_CONNECTION_ERROR = "[try_call()] AMQPConnectionError..."
TRYCALL_RAISE_AMQP_CHANNEL_ERROR = "[try_call()] AMQPChannelError. Raising Exception."
TRYCALL_CONNECTION_ERROR_TRY_AGAIN = "[try_call()] Connection error. Trying again."
TRYCALL_CONNECTION_ERROR_MAX_RETRIES = "[try_call()] Connection error. Reached max retries. Raising Exception."

TRYYIELD_CONNECTION_CLOSED_BY_BROKER = "[try_yield()] ConnectionClosedByBroker..."
TRYYIELD_AMQP_CONNECTION_ERROR = "[try_yield()] AMQPConnectionError..."
TRYYIELD_RAISE_AMQP_CHANNEL_ERROR = "[try_yield()] AMQPChannelError. Raising Exception."
TRYYIELD_CONNECTION_ERROR_TRY_AGAIN = "[try_yield()] Connection error. Trying again."
TRYYIELD_CONNECTION_ERROR_MAX_RETRIES = "[try_yield()] Connection error. Reached max retries. Raising Exception."
