"""Common logging strings."""

INIT_PUB = "[__init__()] Initializing pub."
INIT_SUB = "[__init__()] Initializing sub."

CONNECTING_PUB = "[connect()] Pub connecting..."
CONNECTED_PUB = "[connect()] Pub connected."
CONNECTING_SUB = "[connect()] Sub connecting..."
CONNECTED_SUB = "[connect()] Sub connected."

SENDING_MESSAGE = "[send_message()] Sending message..."
SENT_MESSAGE = "[send_message()] Sent message."

CLOSED_PUB = "[close()] Pub closed."
CLOSING_PUB = "[close()] Closing pub..."
CLOSED_SUB = "[close()] Sub closed."
CLOSING_SUB = "[close()] Closing sub..."

GETMSG_RECEIVE_MESSAGE = "[get_message()] Trying to receive message..."
GETMSG_RECEIVED_MESSAGE = "[get_message()] Received message."
GETMSG_NO_MESSAGE = "[get_message()] Didn't receive message. Returning None."
GETMSG_TIMEOUT_ERROR = "[get_message()] Timeout error. Returning None."
GETMSG_CONNECTION_ERROR_TRY_AGAIN = "[get_message()] Connection error. Trying again."
GETMSG_RAISE_OTHER_ERROR = "[get_message()] Other error. Raising Exception."
GETMSG_CONNECTION_ERROR_MAX_RETRIES = (
    "[get_message()] Connection error. Reached max retries. Raising Exception."
)

ACKING_MESSAGE = "[ack_message()] Ack'ing message..."
ACKED_MESSAGE = "[ack_message()] Ack'd message."

NACKING_MESSAGE = "[reject_message()] Nack'ing message..."
NACKED_MESSAGE = "[reject_message()] Nack'd message."

MSGGEN_ENTERED = "[message_generator()] Entered generator."
MSGGEN_GET_NEW_MESSAGE = "[message_generator()] Getting a new message..."
MSGGEN_NO_MESSAGE_LOOK_BACK_IN_QUEUE = (
    "[message_generator()] No messages in idle timeout window."
)
MSGGEN_YIELDING_MESSAGE = "[message_generator()] Yielding message..."
MSGGEN_DOWNSTREAM_ERROR = "[message_generator()] There was a downstream error."
MSGGEN_PROPAGATING_ERROR = "[message_generator()] Propagating error..."
MSGGEN_EXCEPTED_DOWNSTREAM_ERROR = (
    "[message_generator()] Excepted downstream error (not re-raising):"
)
MSGGEN_GENERATOR_EXITING = "[message_generator()] Exiting generator..."
MSGGEN_GENERATOR_EXITED = "[message_generator()] Exited generator."
MSGGEN_CLOSED_QUEUE = "[message_generator()] Closed queue."
