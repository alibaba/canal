package com.alibaba.otter.canal.parse.exception;

import java.io.IOException;

/**
 * @author lulin on 2018/9/14 下午4:54
 * @since 1.1.0
 */

public class PositionErrorException extends IOException {
	private static final long serialVersionUID = -5989982973281276473L;
	public PositionErrorException(String message) {
		super(message);
	}
}
