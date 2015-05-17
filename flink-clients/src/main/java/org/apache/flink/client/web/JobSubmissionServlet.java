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


package org.apache.flink.client.web;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.flink.client.program.Client;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.plan.FlinkPlan;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.StreamingPlan;
import org.apache.flink.optimizer.plandump.PlanJSONDumpGenerator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JobSubmissionServlet extends HttpServlet {

	/**
	 * Serial UID for serialization interoperability.
	 */
	private static final long serialVersionUID = 8447312301029847397L;

	// ------------------------------------------------------------------------

	public static final String START_PAGE_URL = "launch.html";

	private static final String ACTION_PARAM_NAME = "action";

	private static final String ACTION_SUBMIT_VALUE = "submit";

	private static final String ACTION_RUN_SUBMITTED_VALUE = "runsubmitted";

	private static final String ACTION_BACK_VALUE = "back";

	private static final String JOB_PARAM_NAME = "job";

	private static final String CLASS_PARAM_NAME = "assemblerClass";

	private static final String ARGUMENTS_PARAM_NAME = "arguments";

	private static final String SHOW_PLAN_PARAM_NAME = "show_plan";

	private static final String SUSPEND_PARAM_NAME = "suspend";

	private static final Logger LOG = LoggerFactory.getLogger(JobSubmissionServlet.class);

	// ------------------------------------------------------------------------

	private final File jobStoreDirectory;				// the directory containing the uploaded jobs

	private final File planDumpDirectory;				// the directory to dump the optimizer plans to

	private final Map<Long, JobGraph> submittedJobs;	// map from UIDs to the running jobs

	private final Random rand;							// random number generator for UID

	private final Configuration config;


	public JobSubmissionServlet(Configuration config, File jobDir, File planDir) {
		this.config = config;
		this.jobStoreDirectory = jobDir;
		this.planDumpDirectory = planDir;

		this.submittedJobs = Collections.synchronizedMap(new HashMap<Long, JobGraph>());

		this.rand = new Random(System.currentTimeMillis());
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		String action = req.getParameter(ACTION_PARAM_NAME);
		if (checkParameterSet(resp, action, "action")) {
			return;
		}

		// decide according to the action
		if (action.equals(ACTION_SUBMIT_VALUE)) {
			// --------------- submit a job -------------------

			// get the parameters
			String jobName = req.getParameter(JOB_PARAM_NAME);
			String assemblerClass = req.getParameter(CLASS_PARAM_NAME);
			String args = req.getParameter(ARGUMENTS_PARAM_NAME);
			String showPlan = req.getParameter(SHOW_PLAN_PARAM_NAME);
			String suspendPlan = req.getParameter(SUSPEND_PARAM_NAME);

			// check that all parameters are set
			// do NOT check 'assemblerClass' -> it is OK if it is not set
			if (checkParameterSet(resp, jobName, JOB_PARAM_NAME)
				|| checkParameterSet(resp, args, ARGUMENTS_PARAM_NAME)
				|| checkParameterSet(resp, showPlan, SHOW_PLAN_PARAM_NAME)
				|| checkParameterSet(resp, suspendPlan, SUSPEND_PARAM_NAME))
			{
				return;
			}

			boolean show = Boolean.parseBoolean(showPlan);
			boolean suspend = Boolean.parseBoolean(suspendPlan);

			// check, if the jar exists
			File jarFile = new File(jobStoreDirectory, jobName);
			if (!jarFile.exists()) {
				showErrorPage(resp, "The jar file + '" + jarFile.getPath() + "' does not exist.");
				return;
			}

			// parse the arguments
			List<String> params;
			try {
				params = tokenizeArguments(args);
			} catch (IllegalArgumentException iaex) {
				showErrorPage(resp, "The arguments contain an unterminated quoted string.");
				return;
			}

			int parallelism = -1;
			while(params.size() >= 2) {
				if (params.get(0).equals("-c")) {
					assemblerClass = params.get(1);
					params.remove(0);
					params.remove(0);
				}
				else if (params.get(0).equals("-p")) {
					parallelism = Integer.parseInt(params.get(1));
					params.remove(0);
					params.remove(0);
				}
				else {
					break;
				}
			}

			// create the plan
			String[] options = params.isEmpty() ? new String[0] : params.toArray(new String[params.size()]);
			PackagedProgram program;
			FlinkPlan optPlan;
			Client client;

			try {
				if (assemblerClass == null) {
					program = new PackagedProgram(jarFile, options);
				} else {
					program = new PackagedProgram(jarFile, assemblerClass, options);
				}

				client = new Client(config, program.getUserCodeClassLoader());

				optPlan = client.getOptimizedPlan(program, parallelism);

				if (optPlan == null) {
					throw new Exception("The optimized plan could not be produced.");
				}
			}
			catch (ProgramInvocationException e) {
				// collect the stack trace
				StringWriter sw = new StringWriter();
				PrintWriter w = new PrintWriter(sw);

				if (e.getCause() == null) {
					e.printStackTrace(w);
				} else {
					e.getCause().printStackTrace(w);
				}

				String message = sw.toString();
				message = StringEscapeUtils.escapeHtml4(message);

				showErrorPage(resp, "An error occurred while invoking the program:<br/><br/>"
					+ e.getMessage() + "<br/>"
					+ "<br/><br/><pre>" + message + "</pre>");
				return;
			}
			catch (CompilerException cex) {
				// collect the stack trace
				StringWriter sw = new StringWriter();
				PrintWriter w = new PrintWriter(sw);
				cex.printStackTrace(w);

				String message = sw.toString();
				message = StringEscapeUtils.escapeHtml4(message);

				showErrorPage(resp, "An error occurred in the compiler:<br/><br/>"
					+ cex.getMessage() + "<br/>"
					+ (cex.getCause() != null ? "Caused by: " + cex.getCause().getMessage():"")
					+ "<br/><br/><pre>" + message + "</pre>");
				return;
			}
			catch (Throwable t) {
				// collect the stack trace
				StringWriter sw = new StringWriter();
				PrintWriter w = new PrintWriter(sw);
				t.printStackTrace(w);

				String message = sw.toString();
				message = StringEscapeUtils.escapeHtml4(message);

				showErrorPage(resp, "An unexpected error occurred:<br/><br/>" + t.getMessage() + "<br/><br/><pre>"
					+ message + "</pre>");
				return;
			}

			// redirect according to our options
			if (show) {
				// we have a request to show the plan

				// create a UID for the job
				Long uid;
				do {
					uid = Math.abs(this.rand.nextLong());
				} while (this.submittedJobs.containsKey(uid));

				// dump the job to a JSON file
				String planName = uid + ".json";
				File jsonFile = new File(this.planDumpDirectory, planName);

				if (optPlan instanceof StreamingPlan) {
					((StreamingPlan) optPlan).dumpStreamingPlanAsJSON(jsonFile);
				}
				else {
					PlanJSONDumpGenerator jsonGen = new PlanJSONDumpGenerator();
					jsonGen.setEncodeForHTML(true);
					jsonGen.dumpOptimizerPlanAsJSON((OptimizedPlan) optPlan, jsonFile);
				}

				// submit the job only, if it should not be suspended
				if (!suspend) {
					if (optPlan instanceof OptimizedPlan) {
						try {
							client.run(program, (OptimizedPlan) optPlan, false);
						}
						catch (Throwable t) {
							LOG.error("Error submitting job to the job-manager.", t);
							showErrorPage(resp, t.getMessage());
							return;
						}
						finally {
							program.deleteExtractedLibraries();
						}
					}
					else {
						throw new RuntimeException("Not implemented for Streaming Job plans");
					}
				}
				else {
					try {
						this.submittedJobs.put(uid, client.getJobGraph(program, optPlan));
					}
					catch (ProgramInvocationException piex) {
						LOG.error("Error creating JobGraph from optimized plan.", piex);
						showErrorPage(resp, piex.getMessage());
						return;
					}
					catch (Throwable t) {
						LOG.error("Error creating JobGraph from optimized plan.", t);
						showErrorPage(resp, t.getMessage());
						return;
					}
				}

				// redirect to the plan display page
				resp.sendRedirect("showPlan?id=" + uid + "&suspended=" + (suspend ? "true" : "false"));
			}
			else {
				// don't show any plan. directly submit the job and redirect to the
				// runtime monitor
				try {
					client.run(program, parallelism, false);
				}
				catch (Exception ex) {
					LOG.error("Error submitting job to the job-manager.", ex);
					// HACK: Is necessary because Message contains whole stack trace
					String errorMessage = ex.getMessage().split("\n")[0];
					showErrorPage(resp, errorMessage);
					return;
				}
				finally {
					program.deleteExtractedLibraries();
				}
				resp.sendRedirect(START_PAGE_URL);
			}
		}
		else if (action.equals(ACTION_RUN_SUBMITTED_VALUE)) {
			// --------------- run a job that has been submitted earlier, but was -------------------
			// --------------- not executed because of a plan display -------------------

			String id = req.getParameter("id");
			if (checkParameterSet(resp, id, "id")) {
				return;
			}

			Long uid = null;
			try {
				uid = Long.parseLong(id);
			} catch (NumberFormatException nfex) {
				showErrorPage(resp, "An invalid id for the job was provided.");
				return;
			}

			// get the retained job
			JobGraph job = submittedJobs.remove(uid);
			if (job == null) {
				resp.sendError(HttpServletResponse.SC_BAD_REQUEST,
					"No job with the given uid was retained for later submission.");
				return;
			}

			// submit the job
			try {
				Client client = new Client(config, getClass().getClassLoader());
				client.run(job, false);
			}
			catch (Exception ex) {
				LOG.error("Error submitting job to the job-manager.", ex);
				resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
				// HACK: Is necessary because Message contains whole stack trace
				String errorMessage = ex.getMessage().split("\n")[0];
				resp.getWriter().print(errorMessage);
				// resp.sendError(HttpServletResponse.SC_BAD_REQUEST, ex.getMessage());
				return;
			}

			// redirect to the start page
			resp.sendRedirect(START_PAGE_URL);
		} else if (action.equals(ACTION_BACK_VALUE)) {
			// remove the job from the map

			String id = req.getParameter("id");
			if (checkParameterSet(resp, id, "id")) {
				return;
			}

			Long uid = null;
			try {
				uid = Long.parseLong(id);
			} catch (NumberFormatException nfex) {
				showErrorPage(resp, "An invalid id for the job was provided.");
				return;
			}

			// remove the retained job
			submittedJobs.remove(uid);

			// redirect to the start page
			resp.sendRedirect(START_PAGE_URL);
		} else {
			showErrorPage(resp, "Invalid action specified.");
			return;
		}
	}

	/**
	 * Prints the error page, containing the given message.
	 * 
	 * @param resp
	 *        The response handler.
	 * @param message
	 *        The message to display.
	 * @throws IOException
	 *         Thrown, if the error page could not be printed due to an I/O problem.
	 */
	private void showErrorPage(HttpServletResponse resp, String message) throws IOException {
		resp.setStatus(HttpServletResponse.SC_OK);
		resp.setContentType(GUIServletStub.CONTENT_TYPE_HTML);

		PrintWriter writer = resp.getWriter();

		writer
			.println("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\"\n        \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">");
		writer.println("<html>");
		writer.println("<head>");
		writer.println("  <title>Launch Job - Error</title>");
		writer.println("  <meta http-equiv=\"content-type\" content=\"text/html; charset=UTF-8\" />");
		writer.println("  <link rel=\"stylesheet\" type=\"text/css\" href=\"css/nephelefrontend.css\" />");
		writer.println("</head>");

		writer.println("<body>");
		writer.println("  <div class=\"mainHeading\">");
		writer.println("    <h1><img src=\"img/flink-logo.png\" width=\"100\" height=\"100\" alt=\"Flink Logo\" align=\"middle\"/>Flink Web Submission Client</h1>");
		writer.println("  </div>");
		writer.println("  <div style=\"margin-top: 50px; text-align: center;\">");
		writer.println("    <p class=\"error_text\" style=\"font-size: 18px;\">");
		writer.println(message);
		writer.println("    </p><br/><br/>");
		writer.println("    <form action=\"launch.html\" method=\"GET\">");
		writer.println("      <input type=\"submit\" value=\"back\">");
		writer.println("    </form>");
		writer.println("  </div>");
		writer.println("</body>");
		writer.println("</html>");
	}

	/**
	 * Checks the given parameter. If it is null, it prints the error page.
	 * 
	 * @param resp
	 *        The response handler.
	 * @param parameter
	 *        The parameter to check.
	 * @param parameterName
	 *        The name of the parameter, to describe it in the error message.
	 * @return True, if the parameter is null, false otherwise.
	 * @throws IOException
	 *         Thrown, if the error page could not be printed.
	 */
	private boolean checkParameterSet(HttpServletResponse resp, String parameter, String parameterName)
			throws IOException {
		if (parameter == null) {
			showErrorPage(resp, "The parameter '" + parameterName + "' is not set.");
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Utility method that takes the given arguments, splits them at the whitespaces (space and tab) and
	 * turns them into an array of Strings. Other than the <tt>StringTokenizer</tt>, this method
	 * takes care of quotes, such that quoted passages end up being one string.
	 * 
	 * @param args
	 *        The string to be split.
	 * @return The array of split strings.
	 */
	private static final List<String> tokenizeArguments(String args) {
		List<String> list = new ArrayList<String>();
		StringBuilder curr = new StringBuilder();

		int pos = 0;
		boolean quoted = false;

		while (pos < args.length()) {
			char c = args.charAt(pos);
			if ((c == ' ' || c == '\t') && !quoted) {
				if (curr.length() > 0) {
					list.add(curr.toString());
					curr.setLength(0);
				}
			} else if (c == '"') {
				quoted = !quoted;
			} else {
				curr.append(c);
			}

			pos++;
		}

		if (quoted) {
			throw new IllegalArgumentException("Unterminated quoted string.");
		}

		if (curr.length() > 0) {
			list.add(curr.toString());
		}

		return list;
	}
}
