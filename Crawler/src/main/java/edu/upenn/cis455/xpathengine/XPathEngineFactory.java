package edu.upenn.cis455.xpathengine;

import org.xml.sax.helpers.DefaultHandler;

/**
 * (MS2) Implement this factory to produce your XPath engine
 * and SAX handler as necessary.  It may be called by
 * the test/grading infrastructure.
 * 
 * @author cis455
 *
 */
public class XPathEngineFactory {
	
	private static XPathEngine instance = new XPathEngineImpl();
	public static XPathEngine getXPathEngine() {
		return instance;
	}
	
	public static DefaultHandler getSAXHandler() {
		return null;
	}
}
