package net.unit8.jmeter.protocol.websocket.sampler;

import org.apache.commons.lang3.StringUtils;
import org.apache.jmeter.config.Argument;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.config.ConfigTestElement;
import org.apache.jmeter.protocol.http.util.EncoderCache;
import org.apache.jmeter.protocol.http.util.HTTPArgument;
import org.apache.jmeter.protocol.http.util.HTTPConstants;
import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.testelement.TestElement;
import org.apache.jmeter.testelement.TestStateListener;
import org.apache.jmeter.testelement.property.*;
import org.apache.jmeter.threads.JMeterContextService;
import org.apache.jorphan.logging.LoggingManager;
import org.apache.jorphan.util.JOrphanUtils;
import org.apache.log.Logger;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.eclipse.jetty.websocket.WebSocket;
import org.eclipse.jetty.websocket.WebSocketClient;
import org.eclipse.jetty.websocket.WebSocketClientFactory;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

/**
 * The sampler for WebSocket.

 * @author kawasima
 */
public class WebSocketSampler extends AbstractSampler implements TestStateListener {

    private static final Logger log = LoggingManager.getLoggerForClass();

    private static final Set<String> APPLIABLE_CONFIG_CLASSES = new HashSet<String>(
            Arrays.asList(new String[]{
                    "net.unit8.jmeter.protocol.websocket.control.gui.WebSocketSamplerGui",
                    "org.apache.jmeter.config.gui.SimpleConfigGui"}));

    private static final String ARG_VAL_SEP = "="; // $NON-NLS-1$
    private static final String QRY_SEP = "&"; // $NON-NLS-1$
    private static final String QRY_PFX = "?"; // $NON-NLS-1$

    private static final String WS_PREFIX = "ws://"; // $NON-NLS-1$
    private static final String WSS_PREFIX = "wss://"; // $NON-NLS-1$
    private static final String DEFAULT_PROTOCOL = "ws";
    private static final int UNSPECIFIED_PORT = 0;
    private static final String UNSPECIFIED_PORT_AS_STRING = "0"; // $NON-NLS-1$
    private static final int URL_UNSPECIFIED_PORT = -1;


    private WebSocket.Connection connection = null;
    private static final ConcurrentHashSet<WebSocket.Connection> samplerConnections
            = new ConcurrentHashSet<WebSocket.Connection>();

    private boolean initialized = false;
    private String responseMessage;

    public static final String DOMAIN = "WebSocketSampler.domain";
    public static final String PORT = "WebSocketSampler.port";
    public static final String PATH = "WebSocketSampler.path";
    public static final String PROTOCOL = "WebSocketSampler.protocol";
    public static final String CONTENT_ENCODING = "WebSocketSampler.contentEncoding";
    public static final String ARGUMENTS = "WebSocketSampler.arguments";
    public static final String SEND_MESSAGE = "WebSocketSampler.sendMessage";
    public static final String RECV_MESSAGE = "WebSocketSampler.recvMessage";
    public static final String RECV_TIMEOUT = "WebSocketSampler.recvTimeout";

    private static WebSocketClientFactory webSocketClientFactory = new WebSocketClientFactory();


    public WebSocketSampler() {
        setArguments(new Arguments());
    }

    public void initialize() throws Exception {
        URI uri = getUri();
        WebSocketClient webSocketClient = webSocketClientFactory.newWebSocketClient();
        final WebSocketSampler parent = this;
        final String threadName = JMeterContextService.getContext().getThread().getThreadName();
        final Pattern regex = (getRecvMessage() != null) ? Pattern.compile(getRecvMessage()) : null;
        Future<WebSocket.Connection> futureConnection = webSocketClient.open(uri, new WebSocket.OnTextMessage() {

            @Override
            public void onMessage(String s) {
                synchronized (parent) {
                    if (regex == null || regex.matcher(s).find()) {
                        responseMessage = s;
                        parent.notify();
                    }
                }
            }

            @Override
            public void onOpen(Connection connection) {
                log.debug("Connect " + threadName);
            }

            @Override
            public void onClose(int i, String s) {
                log.debug("Disconnect " + threadName);
            }
        });
        connection = futureConnection.get();
        samplerConnections.add(connection);
        initialized = true;
    }
    @Override
    public SampleResult sample(Entry entry) {
        SampleResult res = new SampleResult();
        res.setSampleLabel(getName());

        boolean isOK = false;
        if (!initialized) {
            try {
                initialize();
            } catch (Exception e) {
                res.setResponseMessage(e.getMessage());
                res.setSuccessful(false);
                return res;
            }
        }
        String message = getPropertyAsString(SEND_MESSAGE, "default message");
        res.setSamplerData(message);
        res.sampleStart();
        try {
            if (connection.isOpen()) {
                res.setDataEncoding(getContentEncoding());
                connection.sendMessage(message);
            } else {
                initialize();
            }
            synchronized (this) {
                wait(getRecvTimeout());
            }
            if (responseMessage == null) {
                res.setResponseCode("204");
                throw new TimeoutException("No content (probably timeout).");
            }
            res.setResponseCodeOK();
            res.setResponseData(responseMessage, getContentEncoding());
            isOK = true;
        } catch (Exception e) {
            log.debug(e.getMessage());
            res.setResponseMessage(e.getMessage());
        }
        res.sampleEnd();
        res.setSuccessful(isOK);

        return res;
    }


    @Override
    public void setName(String name) {
        if (name != null)
            setProperty(TestElement.NAME, name);
    }

    @Override
    public String getName() {
        return getPropertyAsString(TestElement.NAME);
    }

    @Override
    public void setComment(String comment){
        setProperty(new StringProperty(TestElement.COMMENTS, comment));
    }

    @Override
    public String getComment(){
        return getProperty(TestElement.COMMENTS).getStringValue();
    }

    public URI getUri() throws URISyntaxException {
        String path = this.getPath();
        // Hack to allow entire URL to be provided in host field
        if (path.startsWith(WS_PREFIX)
                || path.startsWith(WSS_PREFIX)){
            return new URI(path);
        }
        String domain = getDomain();
        String protocol = getProtocol();
        // HTTP URLs must be absolute, allow file to be relative
        if (!path.startsWith("/")){ // $NON-NLS-1$
            path = "/" + path; // $NON-NLS-1$
        }

        String queryString = getQueryString(getContentEncoding());
        if(isProtocolDefaultPort()) {
            return new URI(protocol, null, domain, -1, path, queryString, null);
        }
        return new URI(protocol, null, domain, getPort(), path, queryString, null);
    }

    public void setPath(String path, String contentEncoding) {
        boolean fullUrl = path.startsWith(WS_PREFIX) || path.startsWith(WSS_PREFIX);
        if (!fullUrl) {
            int index = path.indexOf(QRY_PFX);
            if (index > -1) {
                setProperty(PATH, path.substring(0, index));
                // Parse the arguments in querystring, assuming specified encoding for values
                parseArguments(path.substring(index + 1), contentEncoding);
            } else {
                setProperty(PATH, path);
            }
        } else {
            setProperty(PATH, path);
        }
    }

    public String getPath() {
        String p = getPropertyAsString(PATH);
        return encodeSpaces(p);
    }

    public void setPort(int value) {
        setProperty(new IntegerProperty(PORT, value));
    }

    public static int getDefaultPort(String protocol,int port){
        if (port==URL_UNSPECIFIED_PORT){
            return
                    protocol.equalsIgnoreCase(HTTPConstants.PROTOCOL_HTTP)  ? HTTPConstants.DEFAULT_HTTP_PORT :
                            protocol.equalsIgnoreCase(HTTPConstants.PROTOCOL_HTTPS) ? HTTPConstants.DEFAULT_HTTPS_PORT :
                                    port;
        }
        return port;
    }

    /**
     * Get the port number from the port string, allowing for trailing blanks.
     *
     * @return port number or UNSPECIFIED_PORT (== 0)
     */
    public int getPortIfSpecified() {
        String port_s = getPropertyAsString(PORT, UNSPECIFIED_PORT_AS_STRING);
        try {
            return Integer.parseInt(port_s.trim());
        } catch (NumberFormatException e) {
            return UNSPECIFIED_PORT;
        }
    }

    /**
     * Tell whether the default port for the specified protocol is used
     *
     * @return true if the default port number for the protocol is used, false otherwise
     */
    public boolean isProtocolDefaultPort() {
        final int port = getPortIfSpecified();
        final String protocol = getProtocol();
        return port == UNSPECIFIED_PORT ||
                ("ws".equalsIgnoreCase(protocol) && port == HTTPConstants.DEFAULT_HTTP_PORT) ||
                ("wss".equalsIgnoreCase(protocol) && port == HTTPConstants.DEFAULT_HTTPS_PORT);
    }

    public int getPort() {
        final int port = getPortIfSpecified();
        if (port == UNSPECIFIED_PORT) {
            String prot = getProtocol();
            if ("wss".equalsIgnoreCase(prot)) {
                return HTTPConstants.DEFAULT_HTTPS_PORT;
            }
            if (!"ws".equalsIgnoreCase(prot)) {
                log.warn("Unexpected protocol: "+prot);
                // TODO - should this return something else?
            }
            return HTTPConstants.DEFAULT_HTTP_PORT;
        }
        return port;
    }


    public void setDomain(String value) {
        setProperty(DOMAIN, value);
    }

    public String getDomain() {
        return getPropertyAsString(DOMAIN);
    }
    public void setProtocol(String value) {
        setProperty(PROTOCOL, value.toLowerCase(java.util.Locale.ENGLISH));
    }

    public String getProtocol() {
        String protocol = getPropertyAsString(PROTOCOL);
        if (protocol == null || protocol.length() == 0 ) {
            return DEFAULT_PROTOCOL;
        }
        return protocol;
    }

    public void setContentEncoding(String charsetName) {
        setProperty(CONTENT_ENCODING, charsetName);
    }
    public String getContentEncoding() {
        return getPropertyAsString(CONTENT_ENCODING);
    }

    public String getQueryString(String contentEncoding) {
        // Check if the sampler has a specified content encoding
        if(JOrphanUtils.isBlank(contentEncoding)) {
            // We use the encoding which should be used according to the HTTP spec, which is UTF-8
            contentEncoding = EncoderCache.URL_ARGUMENT_ENCODING;
        }
        StringBuilder buf = new StringBuilder();
        PropertyIterator iter = getArguments().iterator();
        boolean first = true;
        while (iter.hasNext()) {
            HTTPArgument item = null;
            Object objectValue = iter.next().getObjectValue();
            try {
                item = (HTTPArgument) objectValue;
            } catch (ClassCastException e) {
                item = new HTTPArgument((Argument) objectValue);
            }
            final String encodedName = item.getEncodedName();
            if (encodedName.length() == 0) {
                continue; // Skip parameters with a blank name (allows use of optional variables in parameter lists)
            }
            if (!first) {
                buf.append(QRY_SEP);
            } else {
                first = false;
            }
            buf.append(encodedName);
            if (item.getMetaData() == null) {
                buf.append(ARG_VAL_SEP);
            } else {
                buf.append(item.getMetaData());
            }

            // Encode the parameter value in the specified content encoding
            try {
                buf.append(item.getEncodedValue(contentEncoding));
            }
            catch(UnsupportedEncodingException e) {
                log.warn("Unable to encode parameter in encoding " + contentEncoding + ", parameter value not included in query string");
            }
        }
        return buf.toString();
    }

    public void setSendMessage(String value) {
        setProperty(SEND_MESSAGE, value);
    }

    public String getSendMessage() {
        return getPropertyAsString(SEND_MESSAGE);
    }

    public void setRecvMessage(String value) {
        setProperty(RECV_MESSAGE, value);
    }

    public String getRecvMessage() {
        return getPropertyAsString(RECV_MESSAGE);
    }

    public void setRecvTimeout(long value) {
        setProperty(new LongProperty(RECV_TIMEOUT, value));
    }

    public long getRecvTimeout() {
        return getPropertyAsLong(RECV_TIMEOUT, 20000L);
    }

    public void setArguments(Arguments value) {
        setProperty(new TestElementProperty(ARGUMENTS, value));
    }


    public Arguments getArguments() {
        return (Arguments) getProperty(ARGUMENTS).getObjectValue();
    }

    protected String encodeSpaces(String path) {
        return JOrphanUtils.replaceAllChars(path, ' ', "%20"); // $NON-NLS-1$
    }

    public void parseArguments(String queryString, String contentEncoding) {
        String[] args = JOrphanUtils.split(queryString, QRY_SEP);
        for (int i = 0; i < args.length; i++) {
            // need to handle four cases:
            // - string contains name=value
            // - string contains name=
            // - string contains name
            // - empty string

            String metaData; // records the existance of an equal sign
            String name;
            String value;
            int length = args[i].length();
            int endOfNameIndex = args[i].indexOf(ARG_VAL_SEP);
            if (endOfNameIndex != -1) {// is there a separator?
                // case of name=value, name=
                metaData = ARG_VAL_SEP;
                name = args[i].substring(0, endOfNameIndex);
                value = args[i].substring(endOfNameIndex + 1, length);
            } else {
                metaData = "";
                name=args[i];
                value="";
            }
            if (name.length() > 0) {
                // If we know the encoding, we can decode the argument value,
                // to make it easier to read for the user
                if(!StringUtils.isEmpty(contentEncoding)) {
                    addEncodedArgument(name, value, metaData, contentEncoding);
                }
                else {
                    // If we do not know the encoding, we just use the encoded value
                    // The browser has already done the encoding, so save the values as is
                    addNonEncodedArgument(name, value, metaData);
                }
            }
        }
    }
    public void addEncodedArgument(String name, String value, String metaData, String contentEncoding) {
        if (log.isDebugEnabled()){
            log.debug("adding argument: name: " + name + " value: " + value + " metaData: " + metaData + " contentEncoding: " + contentEncoding);
        }

        HTTPArgument arg = null;
        final boolean nonEmptyEncoding = !StringUtils.isEmpty(contentEncoding);
        if(nonEmptyEncoding) {
            arg = new HTTPArgument(name, value, metaData, true, contentEncoding);
        }
        else {
            arg = new HTTPArgument(name, value, metaData, true);
        }

        // Check if there are any difference between name and value and their encoded name and value
        String valueEncoded = null;
        if(nonEmptyEncoding) {
            try {
                valueEncoded = arg.getEncodedValue(contentEncoding);
            }
            catch (UnsupportedEncodingException e) {
                log.warn("Unable to get encoded value using encoding " + contentEncoding);
                valueEncoded = arg.getEncodedValue();
            }
        }
        else {
            valueEncoded = arg.getEncodedValue();
        }
        // If there is no difference, we mark it as not needing encoding
        if (arg.getName().equals(arg.getEncodedName()) && arg.getValue().equals(valueEncoded)) {
            arg.setAlwaysEncoded(false);
        }
        this.getArguments().addArgument(arg);
    }

    public void addEncodedArgument(String name, String value, String metaData) {
        this.addEncodedArgument(name, value, metaData, null);
    }

    public void addNonEncodedArgument(String name, String value, String metadata) {
        HTTPArgument arg = new HTTPArgument(name, value, metadata, false);
        arg.setAlwaysEncoded(false);
        this.getArguments().addArgument(arg);
    }

    public void addArgument(String name, String value) {
        this.getArguments().addArgument(new HTTPArgument(name, value));
    }

    public void addArgument(String name, String value, String metadata) {
        this.getArguments().addArgument(new HTTPArgument(name, value, metadata));
    }

    public boolean hasArguments() {
        return getArguments().getArgumentCount() > 0;
    }

    @Override
    public void testStarted() {
        testStarted("");
    }

    @Override
    public void testStarted(String host) {
        try {
            webSocketClientFactory.start();
        } catch(Exception e) {
            log.error("Can't start WebSocketClientFactory", e);
        }
    }

    @Override
    public void testEnded() {
        testEnded("");
    }

    @Override
    public void testEnded(String host) {
        try {
            for(WebSocket.Connection connection : samplerConnections) {
                connection.close();
            }
            webSocketClientFactory.stop();
        } catch (Exception e) {
            log.error("sampler error when close.", e);
        }
    }

    /**
     * @see org.apache.jmeter.samplers.AbstractSampler#applies(org.apache.jmeter.config.ConfigTestElement)
     */
    @Override
    public boolean applies(ConfigTestElement configElement) {
        String guiClass = configElement.getProperty(TestElement.GUI_CLASS).getStringValue();
        return APPLIABLE_CONFIG_CLASSES.contains(guiClass);
    }

}
