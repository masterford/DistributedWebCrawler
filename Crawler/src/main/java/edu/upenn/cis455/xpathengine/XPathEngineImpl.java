package edu.upenn.cis455.xpathengine;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/** (MS2) Implements XPathEngine to handle XPaths.
 * @author Ransford Antwi
  */
public class XPathEngineImpl implements XPathEngine {
  private String[] XPaths;
  private Stack<Character> stack;
  private HashMap<String, Boolean> validCache;
  private final String AXIS = "/";
  private final String TEST = "[";
  private final String regex =  "^\\s*([a-zA-Z_])+([a-zA-Z0-9\\-\\._])*\\s*$"; //"^[a-zA-z0-9]+$";
  private Pattern pattern;
  private ArrayList<Boolean> results;

  public XPathEngineImpl() {
    // Do NOT add arguments to the constructor!!
	   stack = new Stack<Character>(); //used to keep track of parentheses
	   validCache = new HashMap<String, Boolean>();
	   results = new ArrayList<Boolean>(); //to store results from DOM match recursion
	   pattern = Pattern.compile(regex);
  }
	
  public void setXPaths(String[] s) {
    /*Store the XPath expressions that are given to this method */
	  this.XPaths = s;
  }
  
  /*Helper function to return a queue of XPath tokens  */
  private Queue<String> tokenizeXPath(String s) {
	  Queue<String> tokens = new LinkedList<String>();
	  int length = s.length();
	  for(int i = 0; i < length; i++) {
		  
		  char c = s.charAt(i);
		  if(c == '"') {
			  int start = i;
			  i++;
			  if(i == length) {
				  break;
			  }
			  while(s.charAt(i) != '"') {
				  i++;
				  if(i == length) {
					  break;
				  }
			  }
			  int end = i+1;
			  if(end >= length) {
				  break;
			  }
			  tokens.add(s.substring(start, end));
		  }
		  if(c == '/' || c == '[' || c == ']' || c == '@' || c == '(' || c == ')' || c == ',' || c == '=') { //axis
			  tokens.add(Character.toString(c));
			  continue;
		  }		  
		  if(Character.isAlphabetic(s.codePointAt(i))) { //nodename/attname
			  int start = i;
			  while(Character.isLetterOrDigit(s.charAt(i)) || s.charAt(i) == '_' || s.charAt(i) == ':' || s.charAt(i) == '-') { //tokenize nodename
				  i++;
				  if(i == length) {
					  break;
				  }
			  }
			  int end = i;
			  String token = s.substring(start, end);
			  if(token.equals("text") && i + 1 < length && s.charAt(i) == '(' && s.charAt(i+1) == ')') { //check if token is text()
				  i += 2;
				  end = i;
				  tokens.add(s.substring(start, end));
			  }else {
				  tokens.add(token);
			  }			  
			  i--;
			  continue;
		  }
		  
	  }
	  	  
	  return tokens;
  }
   
  /* Validates whether is is a valid test terminal in the grammer below
   * test  -> step
  * 	   -> text() = "..."
  *       -> contains(text(), "...")
  *       -> @attname = "..."
  *   */
  private boolean checkTest(Queue<String> tokens) { //checks the test step
	  if(tokens.isEmpty()) { //can't end on a test
		  return false;
	  }
	  String current = tokens.poll();
	  if(current.equals("[")) {
		  stack.push('[');
		  return checkTest(tokens);
	  }
	  if(current.equals("]") && !tokens.isEmpty()) {
		  if(stack.isEmpty() || stack.pop() != '[') {
			  return false;
		  }
		  return checkTest(tokens);
	  }
	  if(isValidEnd(current, tokens)) { //check for valid ending
		  return true;
	  }
	  if(tokens.isEmpty()) {
		  return false;
	  }
	  if(current.equals("@")) {
		  tokens.poll();
		  if(tokens.isEmpty() || tokens.size() < 3) {
			  return false;
		  }
		  String next = tokens.poll();
		  if(!next.equals("="))	{
			  return false;
		  }	
		  next = tokens.poll();
		  char c = next.charAt(0);
		  char d = next.charAt(next.length() - 1);
		  if(c != '"' || d != '"') { //has to be enclosed in string literal
			  return false;
		  }
		  return checkTest(tokens);
	  }
	  if(current.equals("contains")) {
		  if(!tokens.poll().equals("(")) {
			  return false;
		  }
		  stack.push('(');
		  return checkTest(tokens);
	  }
	  if(current.equals("text()")){
		  String next = tokens.poll();
		  if(tokens.isEmpty() || tokens.size() < 2) {
			  return false;
		  }		  
		  if(!next.equals("=") && !next.equals(",")) {
			  return false;
		  }
		  next = tokens.poll();
		  char c = next.charAt(0);
		  char d = next.charAt(next.length() - 1);
		  if(c != '"' || d != '"') { //has to be enclosed in string literal
			  return false;
		  }
		  next = tokens.poll();
		  if(next.equals("]")) {
			  stack.pop();
			  if(tokens.isEmpty() && stack.isEmpty()) {
				  return true;
			  }
			  next = tokens.poll();
			  if(next.equals("/")) {
				  return checkStep(tokens);
			  }else if(next.equals("]")) {
				  stack.pop();
				  return checkTest(tokens);
			  }
		  }else if(next.equals(")")) {
			   if(stack.pop() != '(') { //parenthesis don't match
				   return false;
			   }
			   if(tokens.isEmpty()) {
				   return false;
			   }
			   return checkTest(tokens);
		  }else {
			  return false;
		  }
	  }
	  if(tokens.peek().equals("/")) {
		  current = tokens.poll();
	  }
	
	  return checkStep(tokens);
  }
  
  /*
   * Determines whether a String is a valid node name in our XPath Grammer
   */
  public boolean isValidNodeName(String name) {
	  Matcher matcher = pattern.matcher(name);
	  return (matcher.matches() && !name.equals("text()") && !name.equals("contains"));
  }
  
  /*Recursive function to check whether a given xpath is valid based on the grammer below
   * XPath -> axis step
   * axis   -> /   
   * step  -> nodename([test])*(axis step)?  */
  
  private boolean checkStep(Queue<String> tokens) {
	  if(tokens.isEmpty()) { //ended on a node name
		  return true;
	  }
	  String name = tokens.poll();	
	  
	  if(isValidEnd(name, tokens)) {
		  return true;
	  }	 
	  if(name.equals("[")) {
		  stack.push('[');
		  return checkTest(tokens);
	  }
	  if(!isValidNodeName(name)) { //not a valid node name
		  return false;
	  }
	  
	  if(tokens.isEmpty()) { //ended on a node name
		  return true;
	  }
	  String next = tokens.poll();
	  if(next.equals("/")) {
		  if(tokens.isEmpty()) { //can't end on axis
			  return false;
		  }else {
			  if(!isValidNodeName(tokens.peek())) { //not valid name
				  return false;
			  }
			  return checkStep(tokens);
		  }
	  }else if(next.equals("[")){
		  stack.push('[');
		  return checkTest(tokens);
	  } else {
		  if(tokens.size() == 1) {
			  return isValidEnd(next, tokens);
		  }
		  return checkStep(tokens);
	  }	 
  }
  
  /*Helper function used to check if the last parenthesis in the xpath is a valid last character. */
  private boolean isValidEnd(String name, Queue<String> tokens) {
	  return (name.equals("]") && tokens.isEmpty() && !stack.isEmpty() && stack.pop() == '[' && stack.isEmpty());		  
  }
  
  public boolean isValidXPath(String expression) {
	  stack.clear();
	  if(expression == null || expression.isEmpty()) {
		  return false;
	  }
	  Queue<String> tokens = tokenizeXPath(expression);
	  if(tokens.isEmpty() || !tokens.poll().equals("/")) {
		  return false;
	  }
	  if(tokens.isEmpty()) {
		  return false;
	  }
	  return checkStep(tokens);
  }
  
  
  /*Checks whether the ith element of the XPaths array is a valid XPath Expression */
  public boolean isValid(int i) {
    /* Check which of the XPath expressions are valid */
	  if(XPaths == null || XPaths.length == 0) {
		  return false;
	  }
	  stack.clear(); //reset the stack	  
	  if(validCache.containsKey(XPaths[i])) {
		  return validCache.get(XPaths[i]);
	  }else {
		  boolean result = isValidXPath(XPaths[i]);
		  validCache.put(XPaths[i], result);
		  return result;
	  }		
  }

  /*Matches whether the TEST clause of the XPath Expression matches the document
   * @param tokens : the queue consisting of the tokenized xpath expression. This is already a valid Xpath.
   * @param nodes: Narrowed down NodeList of the original document  
   *   */
  private void matchTest(Queue<String> tokensOld, NodeList nodes) {
	  Queue<String> tokens = new LinkedList<String>();
	  tokens.addAll(tokensOld);
	  
	  if(nodes == null && !tokens.isEmpty()) {
		  results.add(false);
		  return;
	  }
	  if(tokens.isEmpty()) {
		  results.add(false);
		  return;
	  }
	  String next = tokens.poll();
	 // System.out.println(next);
	  if(next.equals("contains")) {
		  tokens.poll(); // ( ....already validated the XPath so we know what the next 3 tokens will look like based on the grammer defined in the validate method
		  tokens.poll(); // text()
		  tokens.poll(); // ,
		 String text = tokens.poll();
		 text = text.substring(1, text.length() - 1); //remove quotes around string
		 int length = nodes.getLength();
		 for (int i = 0; i < length; i++) {
			 if(nodes.item(i).getTextContent().contains(text)) { //TODO: Might have to ignore case...				 
				 results.add(true);
				 return;
			 }
		 }
		 tokens.poll(); // ')' token
		 tokens.poll(); // ']' token
		 if(tokens.isEmpty()) { //We didn't find a match within the first TEST clause and there are no more TEST clauses
			results.add(false);
			return;
		 }  else{				  
			  matchTest(tokens, nodes); //TODO: remove?		
			  return;
		 }
	  } else if(next.equals("@")) {
		  String attName = tokens.poll();
		  String attValue = tokens.poll();
		  tokens.poll(); // "]" token
		  attValue = attValue.substring(1, attValue.length() - 1);
		  int length = nodes.getLength();
		  for(int i = 0; i < length; i++) {
			  Element element = (Element) nodes.item(i);
			  if(element.hasAttribute(attName) && element.getAttribute(attName).equals(attValue)) {
					  results.add(true);
					  return;
			  }
		  }
		  if(tokens.isEmpty()) {
			  results.add(false);
			  return;
		  }
	  } else if (next.equals("text()")){
		  tokens.poll(); // "=" token 
		  String text = tokens.poll();
		  text = text.substring(1, text.length() - 1);
		  int length = nodes.getLength();
		  tokens.poll(); // ']' token
		  for(int i = 0; i < length; i++) {			 			 
			  if(nodes.item(i).getTextContent().equals(text)) {
				  if(tokens.isEmpty()) {
					  results.add(true);
					  return;
				  }
			  }
		  }
		  if(tokens.isEmpty()) { //match not found
			  results.add(false);
			  return;
		  }
	  }
	  	  
	  //next is a nodename
	// for(int i = 0; i < nodes.getLength(); i++) {
		// System.out.println(nodes.item(i).getParentNode().getNodeName());
	//	 matchAxisStep(tokens, nodes, (Element) nodes.item(i).getParentNode());
	// }
	 Element root = (Element) nodes.item(0).getParentNode();
	 matchAxisStep(tokens, nodes, root);
	 	  
  }
    
  private void matchAxisStep(Queue<String> tokensOld, NodeList nodes, Element root) {
	  Queue<String> tokens = new LinkedList<String>();
	  tokens.addAll(tokensOld);
	  if(nodes == null && !tokens.isEmpty()) {		
		  //results.add(false);
		  return;
	  }
	  if(tokens.isEmpty()) {
		  return;
	  }
	  String nodeName = tokens.poll();
	  if(nodeName.equals(AXIS)) {
		  if(tokens.isEmpty()) {
			  System.out.println("empt");
			  results.add(false);
			  return;
			 // return false;
		  }
		  NodeList descendants = root.getElementsByTagName(tokens.peek());
		 // NodeList descendants = root.getChildNodes();
		  if(descendants == null) {
			  System.out.println("desc");
			  results.add(false);
			  return;
			  //return false;
		  }
		  matchAxisStep(tokens, descendants, root);
		  return;
	  }
	  if(nodeName.equals(TEST)) {		  
		   matchTest(tokens, nodes);
		   return;
	  }	  
	  int length = nodes.getLength();
	  boolean res = false;
	  for(int i = 0; i < length; i++) {
		  if(nodes.item(i).getNodeName().equals(nodeName)) {
			  res = true;
			  matchAxisStep(tokens, nodes.item(i).getChildNodes(), (Element) nodes.item(i));
		  }
	  }
	  if(!res) {
		  results.add(false);
	  }	  
  }
  private boolean match(Document d, String xpath) { //recursive function
	  if(d == null) {
		  return false; 
	  }
	  Queue<String> tokens = tokenizeXPath(xpath); //We already know this is a valid xpath because this method is only called when the xpath is valid
	  tokens.poll(); // "/" axis
	  String elementName = tokens.poll(); //root element node
	  Element root = d.getDocumentElement();	
	  
	  if(!root.getNodeName().equals(elementName)) {
		  return false;
	  }
	  if(tokens.isEmpty()) {
		  return true;
	  }
	  results.clear();
	  matchAxisStep(tokens, root.getChildNodes(), root);
	  return (results.contains(true) || results.isEmpty())  ;
  }
  
  public boolean[] evaluate(Document d) { 
    /* Check whether the document matches the XPath expressions */
	  if(XPaths == null) {
		  return null;
	  }
	  boolean[] result = new boolean[XPaths.length];
	  for(int i = 0; i < result.length; i++) {
		  result[i] = match(d, XPaths[i]);
	  }
    return result; 
  }

	@Override
	public boolean isSAX() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean[] evaluateSAX(InputStream document, DefaultHandler handler) {
		// TODO Auto-generated method stub
		return null;
	}      
}
