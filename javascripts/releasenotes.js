/*
*	Copyright, Cedric Dugas,
*	Do not sell or redistribute
*/


(function($)
{
	// mardown parser options, do not touch
	marked.setOptions({
		 gfm: true,
		 pedantic: false,
		 sanitize: true
	});

	// Default setting
	 var defaultSettings = {
		milestonesShown			: 10,
		// If you want to show private repo
		// You need to add repo credentials in api.php
		phpApi 				: false,
			phpApiPath		: '/',
		showDescription 			: true,
		showComments 			: true,
		// Used if phpApi is set to false
	     	repo		            		: 'rails',
	     	username       			: 'rails'
	 };

    	$.fn.releaseNotes = function(settings){
	 	settings = $.extend({}, defaultSettings, settings || {});
	 	var apiPath = settings.phpApiPath+"api.php";
	 	var respType = (settings.phpApi) ? "json" : "jsonp";

		var releases = {
			load: function(el){
				var _this = this;
				this.$el = $(el);
				this.loadEvents();

				var options = $.extend(settings, {
					state: "closed",
					action: "milestones",
					sort:"due_date"
				});

			 	this.callApi(options).success(function(resp){
			 		if(resp.data) resp= resp.data;
			 		_this.showMilestones(resp);
			 	}).error(function(resp){
			 		console.log(resp)
			 	});
			},
			loadEvents : function(){
				var _this = this;
				if(settings.showDescription) this.$el.delegate(".issue", "click", function(){ _this.loadIssueDesc(this); return false;});
				if(settings.showComments) this.$el.delegate(".btnComments", "click", function(){ _this.loadComments(this); return false;});
			},
			loadIssueDesc : function(el){
				var $issue = $(el);
				var data = $issue.data("issue");
				$bigIssue = $issue.find(".issueBig");

				if(!$bigIssue.is(":visible")){
					$(".issueBig").not($bigIssue).slideUp();
					$bigIssue.find(".container").html(components.bigIssue(data, settings.showComments));
					$bigIssue.slideDown();
				}else{
					$bigIssue.slideUp();
				}
			},
			showMilestones : function(milestones){
				var _this = this;
				milestones.sort(this.sortDate);
				$.each(milestones, function(i, milestone){
					if(i == settings.milestonesShown) return false;
					milestone.prettyDate = (milestone.due_on) ? _this.getPrettyDate(milestone.due_on) :  _this.getPrettyDate(milestone.created_at);
					_this.$el.append(components.milestone(milestone));
					var options = $.extend(settings, {
						milestone: milestone.number,
						sort:"created",
						action:"issues",
						state:"closed"
					});
					_this.callApi(options).success(function(resp, textStatus, jqXHR){
						if(resp.data) resp= resp.data;
				 		_this.showIssues(resp);
				 	});
				 	
				});
			},
			loadComments : function(el){

				var _this =this;
				var issueid =$(el).attr("data-issue-id");
				$(el).fadeOut().slideUp();

				var options = $.extend(settings, {
					issueid:issueid,
					action:"comments"
				});
				
				this.callApi(options).success(function(resp, textStatus, jqXHR){
					if(resp.data) resp= resp.data;
			 		_this.showComments(resp, issueid);
			 	});
			},
			showComments : function(comments,issueid){
				var _this = this;

				var $commentContainer = _this.$el.find("#issue"+issueid).find(".comments").empty();
				$.each(comments, function(i, comment){
					comment.prettyDate = _this.getPrettyDate(comment.created_at);
					$commentContainer.append(components.comment(comment, issueid));
				});
				$commentContainer.slideDown();
			},
			showIssues : function(issues){
				var _this  = this;
				var jqMilestone = _this.$el.find("[data-id-milestone="+issues[0].milestone.number+"]");
				var jqMilestoneIssues = jqMilestone.find(".issues");
				issues.sort(this.sortLabel);
				$.each(issues, function(i, issue){
					issue.prettyDate = _this.getPrettyDate(issue.closed_at);
					jqMilestoneIssues.append(components.issue(issue));
				});
				jqMilestone.addClass("separator");
				if(settings.showDescription) jqMilestone.find(".issue").addClass("cursor");
			},
			getPrettyDate : function(date){
				var dateFormat = "";
				var date = date.split("T");
				var dateArray = date[0].split("-");
				var dateObj = new Date(dateArray[0],(dateArray[1]-1),dateArray[2]);

				var weekday=new Array("Sunday","Monday","Tuesday","Wednesday","Thursday", "Friday","Saturday");
    				var monthname=new Array("January","February","March","April","May","June","July","August", "September","October","November","December");
    				dateFormat += weekday[dateObj.getDay()] + ", ";
   		  		dateFormat += monthname[dateObj.getMonth()] + " ";
   		  		dateFormat += dateObj.getDate()  + ", ";
    				dateFormat += dateObj.getFullYear();
    				return dateFormat;
			},
			sortDate : function (milestone1, milestone2) {

				milestone1.dateTest = (milestone1.due_on) ? milestone1.due_on :  milestone1.created_at;
				milestone2.dateTest = (milestone2.due_on) ? milestone2.due_on :  milestone2.created_at;

				var date1 = new Date(milestone1.dateTest );
				var date2 = new Date(milestone2.dateTest );

				  if (date1 < date2) return 1;
				  if (date1 > date2) return -1;
				  return 0;
			},
			sortLabel :function(thisObject,thatObject) {
				if(!thisObject.labels.length) return 1;
				if(!thatObject.labels.length) return -1;
				if (thisObject.labels[0].name > thatObject.labels[0].name){
					return 1;
				}
				else if (thisObject.labels[0].name < thatObject.labels[0].name){
					return -1;
				}
				return 0;
			  },
			callApi: function(options){
				var myoption = $.extend({}, options);
				if(myoption.repo) delete myoption.repo;
				return $.ajax({
					url:this.urls[options.action](options),
					dataType:respType,
					data:myoption
				});
			}, 
			urls : {
				domainName : "https://api.github.com",
				milestones : function(){
					if(!settings.phpApi){
						return $url = this.domainName+ "/repos/"+settings.username +"/"+ settings.repo +"/milestones";
					}else{
						return apiPath;
					};
				},
				issues : function(){
					if(!settings.phpApi){
						return $url = this.domainName+"/repos/"+ settings.username +"/"+ settings.repo +"/issues";
					}else{
						return apiPath;
					};
				},
				comments : function(options){
					if(!settings.phpApi){
						return $url = this.domainName+"/repos/"+ settings.username +"/"+ settings.repo+"/issues/"+ options.issueid +"/comments";
					}else{
						return apiPath;
					}
				}
			}
		};

		return this.each(function(){
			releases.load(this, settings);
		});
    };
    var components = {
	milestone : function(data){
		return $('<div data-id-milestone="'+data.number+'" class="milestoneContainer">    \
			        <h3 class="release">'+data.title+'</h3>\
			        <p class="dateRelease">Release Date: '+data.prettyDate+'</p>\
			        <div class="issues"></div>\
			  </div>').data("milestone", data);
	},
	issue : function(data){
		var 	labels = "",
			_this	 = this;

		$.each(data.labels, function(i, label){   labels += _this.label(label);	});
		return $('<div id="issue'+data.number+'" class="issue" >\
				<div class="issueSmall">\
					<div class="issueTitle">'+labels+' '+$("<span>").html(data.title).text()+' </div>\
				</div>\
				<div style="display:none;" class="issueBig">\
					<div class="container"></div>\
					<div style="display:none;" class="comments"></div>\
				</div>').data("issue", data);
	},
	label : function(data){
		return '<span style="background-color:#'+data.color+';" class="label">'+data.name+'</span>';
	},
	bigIssue : function(data, showComment){
		var commentLink = (data.comments != 0 && showComment) ? '<a data-issue-id="'+data.number+'" href="#" class="btnComments btn">See Comments</a>' : "";

		return $('<div class="avatar-bubble js-comment-container">\
	            <span class="avatar"><img height="48" src="'+data.user.avatar_url+'" width="48"></span>\
	            <div class="bubble">\
	              <div class="comment starting-comment adminable" id="issue-3542089">\
	                <div class="body">\
	                  <p class="author">Opened by <a href="'+data.user.url+'">'+data.user.login+'</a>, issue closed on '+data.prettyDate+'</p>\
	                  <h2 class="content-title"> '+$("<span>").html(data.title).text()+'</h2>\
	                  <div class="formatted-content"> \
	                    <div class="content-body markdown-body markdown-format">\
	                        '+marked(data.body)+'\
	                    </div>\
	                  </div>\
	                </div>\
	              </div>\
	            </div>\
	          </div>'+commentLink).data("issue", data);
	},
	comment : function(data){
		return '<div class="avatar-bubble js-comment-container">\
				    <span class="avatar"><img height="48" src="'+data.user.avatar_url+'" width="48"></span>\
				    <div class="bubble">\
				<div id="issuecomment-4369977" class="comment normal-comment adminable">\
				  <div class="cmeta">\
				    <p class="author">\
				        <span class="icon"></span>\
				      <strong class="author"><a href="'+data.user.url+'">'+data.user.login+'</a></strong>\
				      <em class="action-text">\
				        commented\
				      </em>\
				    </p>\
				    <p class="info">\
				      <em class="date"><time class="js-relative-date">'+data.prettyDate+'</time></em>\
				    </p>\
				  </div>\
				  <div class="body">\
				    <div class="formatted-content">\
				      <div class="content-body markdown-body markdown-format">'+marked(data.body)+'</div>\
				    </div>\
				  </div>\
				</div>\
				    </div>\
				  </div>';
		}
	}
})(jQuery);