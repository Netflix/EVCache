package com.netflix.evcache.util;

import java.util.Iterator;
import java.util.Set;

public class ZoneFallbackIterator {
	Entry<String> entry;
	int size = 0;

	public ZoneFallbackIterator(Set<String> allZones) {
		if(allZones == null || allZones.size() == 0) return;
		for(Iterator<String> itr = allZones.iterator(); itr.hasNext(); ) {
			final String zone = itr.next();
			final Entry<String> newEntry = new Entry<String>(zone, entry);
			if(entry == null) entry = newEntry;
		}
		
		Entry<String> _entry = entry;
		while(_entry.next != null) {
			_entry = entry.next;
			size++;
		}
		_entry.next = entry;
	}

	public String next() {
		if(entry == null) return null;
		entry = entry.next;
    	return entry.element;
    }

	public String next(String ignoreZone) {
		if(entry == null) return null;
		entry = entry.next;
		if(entry.element.equals(ignoreZone)) {
			return entry.next.element;
		} else {
			return entry.element;
		}
    }
	
	public int getSize() {
		return size;
	}

	class Entry<E> {
		E element;
		Entry<E> next;

		Entry(E element, Entry<E> next) {
		    this.element = element;
		    this.next = next;
		}
	}	
}