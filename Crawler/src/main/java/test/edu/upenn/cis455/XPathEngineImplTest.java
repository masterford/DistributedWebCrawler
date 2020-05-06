package test.edu.upenn.cis455;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import edu.upenn.cis455.xpathengine.XPathEngineImpl;

public class XPathEngineImplTest {
	XPathEngineImpl validengine = new XPathEngineImpl();
	XPathEngineImpl inValidengine = new XPathEngineImpl();

	String[] validXpaths1;
	String[] invalidXpaths1;
	String[] validXpaths2;
	String[] invalidXpaths2;
	Document doc1;
	Document doc2;
	
	@Before
	public void setUp() {
		validXpaths1 = new String [] {"/rss", "/rss/channel", "/rss/channel/title", "/rss/channel/title[contains(text(), \"Sports\")]",  "/rss/channel/title[text() = \"NYT > Sports\"]"};
		validXpaths2 = new String[] {"/rss", "/rss/channel/title[text()=\"NYT > Week in Review\"]", "/rss/channel/link[contains(text(), \"index.html\")]",
				"/rss/channel/item/title[contains(text(), \"The World\")]", "/rss/channel/item/description[contains(text(), \"reverse the fortunes\")]"};
		invalidXpaths1 = new String [] {"/", "/rss/channeldjnd", "/rss/channel/mde", "/rss/channel/title[contains(text(), \"Fordolodo\")]", "/rss/channel/title[text() = \"Fordolodo\"]"};
		invalidXpaths2 = new String[] {"/ford", "/rss/channel/title[text()=\"NYT < Week in Review\"]", "/rss/channel/nope[contains(text(), \"index.html\")]"};
		
		File xml = new File("Sports.xml");
		File xml2 = new File("WeekinReview.xml");
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder;
		try {
			dBuilder = dbFactory.newDocumentBuilder();
			doc1 = dBuilder.parse(xml);
			doc2 = dBuilder.parse(xml2);				
		} catch (ParserConfigurationException e) {		
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}
	
	@Test
	public void testAllValidMatches1() {
		validengine.setXPaths(validXpaths1);
		boolean [] result = validengine.evaluate(doc1);
		for(int i = 0; i < result.length; i++) {
			assertTrue(result[i]);
		}
	}
	
	@Test
	public void testAllInValidMatches1() {
		inValidengine.setXPaths(invalidXpaths1);
		boolean [] result = inValidengine.evaluate(doc1);
		for(int i = 0; i < result.length; i++) {
			assertFalse(result[i]);
		}
	}
	
	@Test
	public void testAllValidMatches2() {
		validengine.setXPaths(validXpaths2);
		boolean [] result = validengine.evaluate(doc2);
		for(int i = 0; i < result.length; i++) {
			assertTrue(result[i]);
		}
	}
	
	@Test
	public void testAllInValidMatches2() {
		inValidengine.setXPaths(invalidXpaths2);
		boolean [] result = inValidengine.evaluate(doc1);
		for(int i = 0; i < result.length; i++) {
			assertFalse(result[i]);
		}
	}
	
	@Test
	public void testAllValidXPaths1() {
		validengine.setXPaths(validXpaths1);
		for(int i = 0; i < validXpaths1.length; i++) {
			assertTrue(validengine.isValid(i));
		}
	}
	
	@Test
	public void testAllValidXPaths2() {
		validengine.setXPaths(validXpaths2);
		for(int i = 0; i < validXpaths2.length; i++) {
			assertTrue(validengine.isValid(i));
		}
	}
	
	@Test
	public void testAllInCorrectXPaths() {
		String[] incorrect = new String[] { "/", "//", "/node/[]"};
		inValidengine.setXPaths(incorrect);
		for(int i = 0; i < incorrect.length; i++) {
			assertFalse(inValidengine.isValid(i));
		}
	}
	
	@Test
	public void testInValidNodeNames() {
		String[] names = new String[] { "/", "//", "/node/[]", "Fordo/", "Fordo[", "@"};
		for(int i = 0; i < names.length; i++) {
			assertFalse(validengine.isValidNodeName(names[i]));
		}
	}
	
	@Test
	public void testValidNodeNames() {
		String[] names = new String[] { "John", "Tom920", "Fordolodo"};
		for(int i = 0; i < names.length; i++) {
			assertTrue(validengine.isValidNodeName(names[i]));
		}
	}
			
	@Test
	public void testValidMatchNested() {
		String [] xpath = new String[] {"/rss/channel/item/title[contains(text(), \"Laugh Lines\")]"};
		validengine.setXPaths(xpath);
		
		boolean [] result = validengine.evaluate(doc2);
		for(int i = 0; i < result.length; i++) {
			assertTrue(result[i]);
		}		
	}

	@Test
	public void xPathValidSimpleTest() {
		assertTrue(validengine.isValidXPath("/foo/bar/xyz"));
	}
	
	@Test
	public void xPathValidTestAtt() {
		assertTrue(validengine.isValidXPath("/foo/bar[@att=\"123\"]"));
	}
	
	@Test
	public void xPathValidContainsTest() {
		assertTrue(validengine.isValidXPath("/foo/bar[contains(text(),\"someSubstring\")]"));
	}
	
	@Test
	public void xPathValidTest4() {
		assertTrue(validengine.isValidXPath("/a/b/c[text()=\"TheEntireText\"]"));
	}
	
	@Test
	public void xPathValidTest5() {
		assertTrue(validengine.isValidXPath("/foo[anotherElement]"));
	}
	
	@Test
	public void xPathValidTest6() {
		assertTrue(validengine.isValidXPath("/this/that[something/else]"));
	}
	
	@Test
	public void xPathValidTest7() {
		assertTrue(validengine.isValidXPath("/d/e/f[foo[text()=\"something\"]][bar]"));
	}
	
	@Test
	public void xPathValidTest8() {
		assertTrue(validengine.isValidXPath("/a/bar/xyz[text() =      \"EveryDayWeLIT\"]"));
	}
	
	@Test
	public void xPathValidTest9() {
		assertFalse(validengine.isValidXPath("//foo/bar/xyz"));
	}
	
}

/**
*@author Ransford Antwi
*/