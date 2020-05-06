package edu.upenn.cis455.crawler;

/** (MS1, MS2) Produces a new XPathCrawler.
  */
public class XPathCrawlerFactory {
	public XPathCrawler getCrawler() {
		return XPathCrawler.getInstance();
	}
}
