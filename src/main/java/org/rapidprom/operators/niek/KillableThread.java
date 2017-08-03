package org.rapidprom.operators.niek;

public class KillableThread extends Thread{
	private volatile boolean keepRunning = true;

    public void run(){
    	while(keepRunning){
    		// do my work
        }
    }

    public void killThread(){
    	keepRunning = false;
        this.interrupt();
    }
}