/**
 * 
 */
package com.google.code.shardbatis.builder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.code.shardbatis.strategy.ShardStrategy;

/**
 * @author sean.he
 * 
 */
public class ShardConfigHolder {
	private static final ShardConfigHolder instance = new ShardConfigHolder();

	public static ShardConfigHolder getInstance() {
		return instance;
	}

	private Map<String, ShardStrategy> strategyRegister = new HashMap<String, ShardStrategy>();

	private Set<String> ignoreSet;
	private Set<String> parseSet;

	private ShardConfigHolder() {
	}

	/**
	 * 注册分表策略
	 * 
	 * @param table
	 * @param strategy
	 */
	public void register(String table, ShardStrategy strategy) {
		this.strategyRegister.put(table.toLowerCase(), strategy);
	}

	/**
	 * 查找对应表的分表策略
	 * 
	 * @param table
	 * @return
	 */
	public ShardStrategy getStrategy(String table) {
		return strategyRegister.get(table.toLowerCase());
	}

	/**
	 * 增加ignore id配置
	 * 
	 * @param id
	 */
	public synchronized void addIgnoreId(String id) {
		if (ignoreSet == null) {
			ignoreSet = new HashSet<String>();
		}
		ignoreSet.add(id);
	}

	/**
	 * 增加parse id配置
	 * 
	 * @param id
	 */
	public synchronized void addParseId(String id) {
		if (parseSet == null) {
			parseSet = new HashSet<String>();
		}
		parseSet.add(id);
	}

	/**
	 * 判断是否配置过parse id<br>
	 * 如果配置过parse id,shardbatis只对parse id范围内的sql进行解析和修改
	 * 
	 * @return
	 */
	public boolean isConfigParseId() {
		return parseSet != null;
	}

	/**
	 * 判断参数ID是否在配置的parse id范围内
	 * 
	 * @param id
	 * @return
	 */
	public boolean isParseId(String id) {
		if (parseSet != null) {
			for (String parsePattern : parseSet) {
				if (wildMatch(parsePattern, id)) {
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * 判断参数ID是否在配置的ignore id范围内
	 * 
	 * @param id
	 * @return
	 */
	public boolean isIgnoreId(String id) {
		if (ignoreSet != null) {
			for (String parsePattern : ignoreSet) {
				if (wildMatch(parsePattern, id)) {
					return true;
				}
			}
		}
		return false;
	}


	public static boolean wildMatch(String pattern, String str) {
		if (pattern == null || str == null) {
			return false;
		}

		boolean result = false;
		char c; // 当前要匹配的字符串
		boolean beforeStar = false; // 是否遇到通配符*
		int back_i = 0;// 回溯,当遇到通配符时,匹配不成功则回溯
		int back_j = 0;
		int i, j;
		for (i = 0, j = 0; i < str.length(); ) {
			if (pattern.length() <= j) {
				if (back_i != 0) {// 有通配符,但是匹配未成功,回溯
					beforeStar = true;
					i = back_i;
					j = back_j;
					back_i = 0;
					back_j = 0;
					continue;
				}
				break;
			}

			if ((c = pattern.charAt(j)) == '*') {
				if (j == pattern.length() - 1) {// 通配符已经在末尾,返回true
					result = true;
					break;
				}
				beforeStar = true;
				j++;
				continue;
			}

			if (beforeStar) {
				if (str.charAt(i) == c) {
					beforeStar = false;
					back_i = i + 1;
					back_j = j;
					j++;
				}
			} else {
				if (c != '?' && c != str.charAt(i)) {
					result = false;
					if (back_i != 0) {// 有通配符,但是匹配未成功,回溯
						beforeStar = true;
						i = back_i;
						j = back_j;
						back_i = 0;
						back_j = 0;
						continue;
					}
					break;
				}
				j++;
			}
			i++;
		}

		if (i == str.length() && j == pattern.length()) {// 全部遍历完毕
			result = true;
		}
		return result;
	}
}
