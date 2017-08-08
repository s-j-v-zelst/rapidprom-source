package org.rapidprom.operators.niek;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Set;

import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;

public class RapidProMThreadCleanUpOperator extends Operator{

	public RapidProMThreadCleanUpOperator(OperatorDescription description) {
		super(description);
	}

	@Override
	public void doWork() throws OperatorException {
		Set<Thread> threads = Thread.getAllStackTraces().keySet();
		for(Thread thread : threads){
			if(thread.getName().startsWith("pool-")){
				thread.interrupt();
				try {
					Method m = Thread.class.getDeclaredMethod( "stop0" , new Class[]{Object.class} );
					m.setAccessible( true );
					try {
						m.invoke(thread , new ThreadDeath() );
					} catch (IllegalAccessException e) {
						e.printStackTrace();
					} catch (IllegalArgumentException e) {
						e.printStackTrace();
					} catch (InvocationTargetException e) {
						e.printStackTrace();
					}
				} catch (NoSuchMethodException e) {
					e.printStackTrace();
				} catch (SecurityException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
