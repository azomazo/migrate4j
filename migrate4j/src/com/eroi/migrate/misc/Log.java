package com.eroi.migrate.misc;

import java.util.logging.Level;
import java.util.logging.Logger;

public class Log {


	Logger l;

	public Log(Class<?> cl) {
		this.l = Logger.getLogger(cl.toString());
	}

	public static Log getLog(Class<?> cl) {
		return new Log(cl);
	}
	
		
	public void error(String msg) {
		this.l.log(Level.SEVERE, msg);
	}

	public void error(String msg, Exception ex) {
		this.l.log(Level.SEVERE, msg, ex);
			
	}

	public void warn(String msg) {
		this.l.log(Level.WARNING, msg);
	}
    
	public void debug(String msg) {
		this.l.log(Level.FINER, msg);
			
	}

	public boolean isDebugEnabled() {
		return l.isLoggable(Level.FINER);
	}


}
