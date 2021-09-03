/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.delegation.hive.copy;

import org.apache.flink.table.planner.delegation.hive.parse.HiveASTHintParser;
import org.apache.flink.table.planner.delegation.hive.parse.HiveASTLexer;
import org.apache.flink.table.planner.delegation.hive.parse.HiveASTParser;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.NoViableAltException;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.Token;
import org.antlr.runtime.TokenRewriteStream;
import org.antlr.runtime.TokenStream;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.CommonTreeAdaptor;
import org.antlr.runtime.tree.TreeAdaptor;
import org.apache.hadoop.hive.ql.parse.ASTErrorNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/** Counterpart of hive's org.apache.hadoop.hive.ql.parse.ParseDriver. */
public class HiveASTParseDriver {

    private static final Logger LOG = LoggerFactory.getLogger(HiveASTParseDriver.class);

    /** ANTLRNoCaseStringStream. */
    // This class provides and implementation for a case insensitive token checker
    // for the lexical analysis part of antlr. By converting the token stream into
    // upper case at the time when lexical rules are checked, this class ensures that the
    // lexical rules need to just match the token with upper case letters as opposed to
    // combination of upper case and lower case characters. This is purely used for matching
    // lexical
    // rules. The actual token text is stored in the same way as the user input without
    // actually converting it into an upper case. The token values are generated by the consume()
    // function of the super class ANTLRStringStream. The LA() function is the lookahead function
    // and is purely used for matching lexical rules. This also means that the grammar will only
    // accept capitalized tokens in case it is run from other tools like antlrworks which
    // do not have the ANTLRNoCaseStringStream implementation.
    public static class ANTLRNoCaseStringStream extends ANTLRStringStream {

        public ANTLRNoCaseStringStream(String input) {
            super(input);
        }

        @Override
        public int LA(int i) {

            int returnChar = super.LA(i);
            if (returnChar == CharStream.EOF) {
                return returnChar;
            } else if (returnChar == 0) {
                return returnChar;
            }

            return Character.toUpperCase((char) returnChar);
        }
    }

    /** HiveLexerX. */
    public static class HiveLexerX extends HiveASTLexer {

        private final ArrayList<HiveASTParseError> errors;

        public HiveLexerX(CharStream input) {
            super(input);
            errors = new ArrayList<>();
        }

        @Override
        public void displayRecognitionError(String[] tokenNames, RecognitionException e) {

            errors.add(new HiveASTParseError(this, e, tokenNames));
        }

        @Override
        public String getErrorMessage(RecognitionException e, String[] tokenNames) {
            String msg = null;

            if (e instanceof NoViableAltException) {
                @SuppressWarnings("unused")
                NoViableAltException nvae = (NoViableAltException) e;
                // for development, can add
                // "decision=<<"+nvae.grammarDecisionDescription+">>"
                // and "(decision="+nvae.decisionNumber+") and
                // "state "+nvae.stateNumber
                msg = "character " + getCharErrorDisplay(e.c) + " not supported here";
            } else {
                msg = super.getErrorMessage(e, tokenNames);
            }

            return msg;
        }

        public ArrayList<HiveASTParseError> getErrors() {
            return errors;
        }
    }

    /**
     * Tree adaptor for making antlr return ASTNodes instead of CommonTree nodes so that the graph
     * walking algorithms and the rules framework defined in ql.lib can be used with the AST Nodes.
     */
    public static final TreeAdaptor ADAPTOR =
            new CommonTreeAdaptor() {
                /**
                 * Creates an HiveParserASTNode for the given token. The HiveParserASTNode is a
                 * wrapper around antlr's CommonTree class that implements the Node interface.
                 *
                 * @param payload The token.
                 * @return Object (which is actually an HiveParserASTNode) for the token.
                 */
                @Override
                public Object create(Token payload) {
                    return new HiveParserASTNode(payload);
                }

                @Override
                public Object dupNode(Object t) {
                    return create(((CommonTree) t).token);
                }

                @Override
                public Object errorNode(
                        TokenStream input, Token start, Token stop, RecognitionException e) {
                    return new ASTErrorNode(input, start, stop, e);
                }
            };

    /**
     * Parses a command, optionally assigning the parser's token stream to the given context.
     *
     * @param command command to parse
     * @param ctx context with which to associate this parser's token stream, or null if either no
     *     context is available or the context already has an existing stream
     * @return parsed AST
     */
    public HiveParserASTNode parse(
            String command, HiveParserContext ctx, String viewFullyQualifiedName)
            throws HiveASTParseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Parsing command: " + command);
        }

        HiveLexerX lexer = new HiveLexerX(new ANTLRNoCaseStringStream(command));
        TokenRewriteStream tokens = new TokenRewriteStream(lexer);
        if (ctx != null) {
            if (viewFullyQualifiedName == null) {
                // Top level query
                ctx.setTokenRewriteStream(tokens);
            } else {
                // It is a view
                ctx.addViewTokenRewriteStream(viewFullyQualifiedName, tokens);
            }
            lexer.setHiveConf(ctx.getConf());
        }
        HiveASTParser parser = new HiveASTParser(tokens);
        if (ctx != null) {
            parser.setHiveConf(ctx.getConf());
        }
        parser.setTreeAdaptor(ADAPTOR);
        HiveASTParser.statement_return r = null;
        try {
            r = parser.statement();
        } catch (RecognitionException e) {
            throw new HiveASTParseException(parser.errors);
        }

        if (lexer.getErrors().size() == 0 && parser.errors.size() == 0) {
            LOG.debug("Parse Completed");
        } else if (lexer.getErrors().size() != 0) {
            throw new HiveASTParseException(lexer.getErrors());
        } else {
            throw new HiveASTParseException(parser.errors);
        }

        HiveParserASTNode tree = r.getTree();
        tree.setUnknownTokenBoundaries();
        return tree;
    }

    /*
     * Parse a string as a query hint.
     */
    public HiveParserASTNode parseHint(String command) throws HiveASTParseException {
        LOG.info("Parsing hint: " + command);

        HiveLexerX lexer = new HiveLexerX(new ANTLRNoCaseStringStream(command));
        TokenRewriteStream tokens = new TokenRewriteStream(lexer);
        HiveASTHintParser parser = new HiveASTHintParser(tokens);
        parser.setTreeAdaptor(ADAPTOR);
        HiveASTHintParser.hint_return r = null;
        try {
            r = parser.hint();
        } catch (RecognitionException e) {
            throw new HiveASTParseException(parser.errors);
        }

        if (lexer.getErrors().size() == 0 && parser.errors.size() == 0) {
            LOG.info("Parse Completed");
        } else if (lexer.getErrors().size() != 0) {
            throw new HiveASTParseException(lexer.getErrors());
        } else {
            throw new HiveASTParseException(parser.errors);
        }

        return r.getTree();
    }
}
