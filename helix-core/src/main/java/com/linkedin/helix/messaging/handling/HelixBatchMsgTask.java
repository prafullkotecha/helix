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
	final MessageHandlerFactory _msgHandlerFty;

	public HelixBatchMsgTask(Message batchMsg, List<Message> msgs, NotificationContext context,
	        MessageHandlerFactory msgHandlerFty) {
		_batchMsg = batchMsg;
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
	public NotificationContext getNotificationContext()
	{
		return _context;
	}

}
