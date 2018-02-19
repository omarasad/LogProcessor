package edu.mcgill.disl.analytics;

import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.Socket;
import java.util.HashMap;

import org.apache.log4j.Logger;

import edu.mcgill.disl.analytics.policy.AppServerPolicy;
import edu.mcgill.disl.analytics.policy.LoadBalancerPolicy;
import edu.mcgill.disl.log.StatisticsManager;

public abstract class Analyser {
	
	public static HashMap<String,String> dns = new HashMap<String,String>();
	
	StatisticsManager manager;
	public static final int LB_PORT = 6788;
	public static final int AS_PORT = 6789;
	
	public static  final Logger log = Logger.getLogger("ObjectAnalyser");
	
	static{
		
		dns.put("disl1", "192.168.1.101");
		dns.put("disl2", "192.168.1.102");
		dns.put("disl3", "192.168.1.103");
		dns.put("disl4", "192.168.1.104");
		dns.put("disl5", "192.168.1.105");
		
		dns.put("disl1.local", "192.168.1.101");
		dns.put("disl2.local", "192.168.1.102");
		dns.put("disl3.local", "192.168.1.103");
		dns.put("disl4.local", "192.168.1.104");
		dns.put("disl5.local", "192.168.1.105");
						
		dns.put("disl", "192.168.1.254");
		
		dns.put("localhost", "127.0.0.1");
		
		// new db cluster
	//	dns.put("db-node-03", "132.206.3.132");
	//	dns.put("db-node-04", "132.206.3.133");
	//	dns.put("db-node-05", "132.206.3.134");
		
		dns.put("db-node-03.CS.McGill.CA", "132.206.3.132");
		dns.put("db-node-04.CS.McGill.CA", "132.206.3.133");
		dns.put("db-node-05.CS.McGill.CA", "132.206.3.134");
		dns.put("db-node-06.CS.McGill.CA", "132.206.3.135");
		
		dns.put("db-node-03", "132.206.3.132");
		dns.put("db-node-04", "132.206.3.133");
		dns.put("db-node-05", "132.206.3.134");
		dns.put("db-node-06", "132.206.3.135");
		
		dns.put("132.206.3.132", "db-node-03.CS.McGill.CA");
		dns.put("132.206.3.133", "db-node-04.CS.McGill.CA");
		dns.put("132.206.3.134", "db-node-05.CS.McGill.CA");
		dns.put("132.206.3.135", "db-node-06.CS.McGill.CA");
		
		
		
		//for hans cluster
		dns.put("node-10", "10.0.1.110");
		dns.put("node-11", "10.0.1.111");
		dns.put("node-12", "10.0.1.112");
		dns.put("node-13", "10.0.1.113");
		dns.put("node-14", "10.0.1.114");
		dns.put("node-15", "10.0.1.115");
		dns.put("node-16", "10.0.1.116");
		dns.put("node-17", "10.0.1.117");
		dns.put("node-18", "10.0.1.118");
		
		
		dns.put("node-10.msdl.lan", "10.0.1.110");
		dns.put("node-11.msdl.lan", "10.0.1.111");
		dns.put("node-12.msdl.lan", "10.0.1.112");
		dns.put("node-13.msdl.lan", "10.0.1.113");
		dns.put("node-14.msdl.lan", "10.0.1.114");
		dns.put("node-15.msdl.lan", "10.0.1.115");
		dns.put("node-16.msdl.lan", "10.0.1.116");
		dns.put("node-17.msdl.lan", "10.0.1.117");
		dns.put("node-18.msdl.lan", "10.0.1.118");
	
		dns.put("10.0.1.110", "node-10.msdl.lan");
		dns.put("10.0.1.111", "node-11.msdl.lan");
		dns.put("10.0.1.112", "node-12.msdl.lan");
		dns.put("10.0.1.113", "node-13.msdl.lan");
		dns.put("10.0.1.114", "node-14.msdl.lan");
		dns.put("10.0.1.115", "node-15.msdl.lan");
		dns.put("10.0.1.116", "node-16.msdl.lan");
		dns.put("10.0.1.117", "node-17.msdl.lan");
		dns.put("10.0.1.118", "node-18.msdl.lan");
	
		
	}
	
	
	public Analyser(StatisticsManager manager){
		this.manager = manager;
	}
	
	public StatisticsManager getManager(){
		return manager;
	}
		
	public abstract void analyse() throws Exception;
	
	public void sendASPolicy(AppServerPolicy apPolicy, String server) throws Exception{
		//sendPolicy(apPolicy, server + ":" + AS_PORT);
		sendPolicy(apPolicy, server);
	}
	
	public void sendLBPolicy(LoadBalancerPolicy lbPolicy, String server) throws Exception{
		//sendPolicy(lbPolicy, server + ":" + LB_PORT);
	//	System.out.println("Omar======lbPolicy:" + lbPolicy);
	//	System.out.println("Omar======server:" + server);
	//	System.out.println("Omar======LB_PORT:" + LB_PORT);
		
		try{
			
		
			Socket sock = new Socket(InetAddress.getByAddress(getIPBytes(server)), LB_PORT);
			sock.setReuseAddress(true);
			// TODO set timeout and catch exceptions
			ObjectOutputStream op = new ObjectOutputStream(sock.getOutputStream());
			op.writeObject(lbPolicy);
			
			op.close();
			sock.close();
			
			//log.info("LoadBalancer Policy test for socket: " + lbPolicy);
			
			}catch(Exception ex){
				log.info(ex.getMessage(),ex);
			}
	}

	private void sendPolicy(Serializable ob, String server) throws Exception{
		
		try{
		// to do AS port
		String[] str = server.split(":");
		//Socket sock = new Socket(InetAddress.getByAddress(getIPBytes(resolveHost(str[0]))), Integer.parseInt(str[1]));
		Socket sock = new Socket(InetAddress.getByAddress(getIPBytes(resolveHost(str[0]))), AS_PORT);
		sock.setReuseAddress(true);
		// TODO set timeout and catch exceptions
		ObjectOutputStream op = new ObjectOutputStream(sock.getOutputStream());
		op.writeObject(ob);
		op.close();
		sock.close();
		
		//log.info("AS Policy: " + ob);
		
		}catch(Exception ex){
			log.info(ex.getMessage(),ex);
		}
	}
	
	public static byte[] getIPBytes(String dottedIp){
		
		String[] ipStr = dottedIp.split("\\.");
		byte[] b = new byte[ipStr.length];
		for(int i=0;i< ipStr.length;i++){
			b[i] = (byte)Integer.parseInt(ipStr[i]);
		}
		return b;
	}
	
	public static String resolveHost(String host){
		
		return dns.get(host);
		
		
	}
	
}
