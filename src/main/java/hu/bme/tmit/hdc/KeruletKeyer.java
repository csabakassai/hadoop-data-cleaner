
package hu.bme.tmit.hdc;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class KeruletKeyer extends Keyer {
	
	private static final Logger log = LoggerFactory.getLogger( KeruletKeyer.class );
	
	static final Pattern punctctrl = Pattern.compile( "\\p{Punct}|[\\x00-\\x08\\x0A-\\x1F\\x7F]" );
	
	@Override
	public String key( String string, Object... params ) {
		String key = string;
		Preconditions.checkNotNull( key );
		key = key.toLowerCase();
		
		if (!key.contains( "budapest" )) {
			log.error( key + " nem budapesti kerulet" );
			return key;
		}
		key = key.replaceAll( "budapest", "" );
		key = punctctrl.matcher( key ).replaceAll( "" ); // then remove all punctuation and control chars
		key = key.replaceAll( "\\(|\\)", "" );
		key = key.replaceAll( "kerulet|kerület|magyarország", "" );
		key = key.replaceAll( "ker", "" );
		key = StringUtils.trim( key );
		
		if (StringUtils.isNumeric( key )) {
			int ker = Integer.parseInt( key );
			String kerString = StringUtils.leftPad( ("" + ker), 2, "0" );
			
			return "BUDAPEST " + kerString;
			
		}
		else if (keruletek.contains( key )) {
			int ker = keruletek.indexOf( key ) + 1;
			String kerString = StringUtils.leftPad( ("" + ker), 2, "0" );
			return "BUDAPEST " + kerString;
			
		}
		else {
			log.error( "nem hatarozhato meg a kerulet: " + string + "; key" + key );
			return string;
		}
		
	}
	
	private static final List<String> keruletek = Lists.newArrayList( "i", "ii", "iii", "iv", "v", "vi", "vii", "viii", "ix", "x",
		"xi", "xii", "xiii", "xiv", "xv", "xvi", "xvii", "xviii", "xix", "xx",
		"xxi", "xxii", "xxiii", "xxiv", "xxv" );
	
	public static void main( String[] args ) {
		KeruletKeyer keruletKeyer = new KeruletKeyer();
		String key = keruletKeyer.key( "BUDAPEST 08" );
		log.info( key );
	}
	
}
