package com.linkedin.helix.messaging.handling;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.model.Message;

public class HelixBatchMsgTask implements MessageTask { // implements
	                                                    // Callable<HelixTaskResult>
	                                                    // {
	final NotificationContext _context;
	final Message _batchMsg;
	final List<Message> _msgs;
	// final MessageHandlerFactory _msgHandlerFty;
	final List<MessageHandler> _handlers;

	public HelixBatchMsgTask(Message batchMsg, List<Message> msgs, List<MessageHandler> handlers,
	        NotificationContext context) {
		_batchMsg = batchMsg;
		_context = context;
		_msgs = msgs;
		_handlers = handlers;
	}

	@Override
	public HelixTaskResult call() throws Exception {
		// for (Message msg : _msgs) {
		for (MessageHandler handler : _handlers) {
			// MessageHandler handler = createMsgHandler(msg, _context);
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

	@Override
	public String getTaskId() {
		StringBuilder sb = new StringBuilder();
		sb.append(_batchMsg.getId());
		sb.append("/");
		List<String> msgIdList = new ArrayList<String>();
		if (_msgs != null) {
			for (Message msg : _msgs) {
				msgIdList.add(msg.getId());
			}
		}
		sb.append(msgIdList);
		return sb.toString();
	}

	@Override
	public Message getMessage() {
		return _batchMsg;
	}

	@Override
	public NotificationContext getNotificationContext() {
		return _context;
	}

	@Override
	public void onTimeout() {
		for (MessageHandler handler : _handlers) {
			if (handler != null) {
				handler.onTimeout();
			}
		}
	}

//	@Override
//	public MessageTask clone() {
//		return new HelixBatchMsgTask(_batchMsg, _msgs, _handlers, _context);
//
//	}
}
