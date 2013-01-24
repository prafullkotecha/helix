package com.linkedin.helix.messaging.handling;

import java.util.List;
import java.util.concurrent.Callable;

import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.model.Message;

public class HelixBatchMsgTask implements Callable<HelixTaskResult> {
	final NotificationContext _context;
	final List<Message> _msgs;
	final MessageHandlerFactory _msgHandlerFty;

	public HelixBatchMsgTask(List<Message> msgs, NotificationContext context,
			MessageHandlerFactory msgHandlerFty) {
		_context = context;
		_msgs = msgs;
		_msgHandlerFty = msgHandlerFty;
	}

	MessageHandler createMsgHandler(Message msg, NotificationContext context) {
		if (_msgHandlerFty == null) {
			// LOG.warn("Fail to find message handler factory for type: " +
			// msgType + " mid:"
			// + message.getMsgId());
			return null;
		}

		return _msgHandlerFty.createHandler(msg, context);
	}

	@Override
	public HelixTaskResult call() throws Exception {
		for (Message msg : _msgs) {
			MessageHandler handler = createMsgHandler(msg, _context);
			if (handler != null) {
				HelixTaskResult result = handler.handleMessage();
				// if any fails, skip the remaining handlers and return fail
				if (!result.isSucess()) {
					return result;
				}
			}
		}
		
		HelixTaskResult result = new HelixTaskResult();
		result.setSuccess(true);
		return result;
	}
}
