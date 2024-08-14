var sidebar = (function (sidebar, $, undefined) {
    var BtnEls = {
        favor: ".js-tplBtn", // 관심기업
        scrap: ".js-scrBtn", // 스크랩
    };

    sidebar.init = {
        exe: function () {
            // 클릭이벤트 연결
            btnEvent();
        }
    };


    // 사이드바 이벤트
    sidebar.event = {

        loginCheck: function () {
            var loginObj;

            $.ajax({
                type: 'post',
                url: '/starter/default/logincheck',
                dataType: 'json',
                cache: false,
                async: false,
                success: function (data) {

                    if (typeof data !== 'undefined') {
                        loginObj = data;
                    }

                }, error: function (e) {

                }
            });

            return loginObj;
        },
        loginOpen: function () {
            if (location.protocol.indexOf("https") >= 0) {
                $.ajax({
                    type: 'post',
                    url: "/starter/default/Starterlogin?actionType=layerPop&reUrl=" + encodeURIComponent(window.location.href).replace(/'/g, "%27").replace(/"/g, "%22"),
                    dataType: 'html',
                    cache: false,
                    success: function (data) {
                        if (data) {
                            $("body").append(data);
                        }
                    }, error: function (e) {
                    }
                });
            } else {
                if (confirm("개인회원 로그인 후 이용해 주세요.\n로그인 페이지로 이동 하시겠습니까?")) {
                    window.location.href = "/Login/Login_tot.asp?re_url=" + encodeURIComponent(window.location.href).replace(/'/g, "%27").replace(/"/g, "%22");
                }
            }
        },
        loginClose: function () {
            $("div.lyOnPassApply").remove();
        },
        loginResize: function (_height) {
            var jQuerydiv = $("body").find("div.lyOnPassApply");
            var jQueryobj = $("#onPassApply_Frame");

            jQueryobj.css("height", _height);

            var isIE = navigator.userAgent.toLowerCase().indexOf("msie") !== -1;

            if (isIE) {
                var $width = window.innerWidth ? window.innerWidth : $(window).width();
                var $height = window.innerHeight ? window.innerHeight : $(window).height();

                var jQueryleft = $(window).scrollLeft() + ($width - jQueryobj.width()) / 2;
                var jQuerytop = $(window).scrollTop() + ($height - jQuerydiv.height()) / 2;
            } else { // 모바일 사파리에서 window 사이즈 틀리게 나와서 window.top으로 수정
                var topWindow = window.top;
                var $width = topWindow.innerWidth ? topWindow.innerWidth : $(topWindow).width();
                var $height = topWindow.innerHeight ? topWindow.innerHeight : $(topWindow).height();

                var scrollTop = $(window).scrollTop();
                var scrollLeft = $(window).scrollLeft();

                if (scrollTop === 0) {
                    scrollTop = $(topWindow).scrollTop();
                }
                if (scrollLeft === 0) {
                    scrollLeft = $(topWindow).scrollLeft();
                }

                var jQueryleft = scrollLeft + ($width - jQueryobj.width()) / 2;
                var jQuerytop = scrollTop + ($height - jQuerydiv.height()) / 2;
            }

            jQuerydiv.css({
                "position": "fixed",
                "z-index": "999999",
                "left": jQueryleft,
                "top": jQuerytop
            });
        },
        loginRefresh: function () {
            var loginObj = sidebar.event.loginCheck();

            if (loginObj && loginObj.RtnCode == -1) {
                sidebar.event.loginOpen();
            } else {
                // lay_Quick_Cls();
                location.reload(true);
            }
        },
        // 채용정보 개편 이벤트 아이콘 노출용 
        go_event_icon_view: function () {
            var _y = document.documentElement.scrollTop;
            if (_y == 0) {
                _y = 140 + document.body.scrollTop;
            } else {
                _y = _y + 140;
            }

            jQuery.ajax({
                type: "GET",
                url: "/Event/2012girenewal/EventIconView.asp",
                dataType: "html",
                success: function (html) {
                    jQuery("#eventIcon").html(html);
                    if (jQuery("#eventIconInn").length > 0) {
                        jQuery("#eventIcon").attr("style", "position:absolute; display:block; z-index:10000;left:50%;top:" + _y + "px");
                    }
                }
            });
        },
        // 크리테오, 타게팅게이츠
        Criteo: function (Gi_No) {
            jQuery.ajax({
                type: "POST",
                url: "/Include/Criteo/Criteo_Scrap_UTF.asp?Gi_No=" + Gi_No,
                dataType: "html",
                success: function (html) {
                    var obj = $(html);
                    $('#widerplanet_tagging').html(obj.html());
                    obj.filter('script').each(function () {
                        $.globalEval(this.text || this.textContent || this.innerHTML || '');
                    });
                }
            });
        },
        //-- NSM Conversion Check
        NsmConversionClick: function (Nsm_Code) {
            var nsmX = new Image();
            //alert(Nsm_Code);
            nsmX.src = "http://ntracker.nsm-corp.com/c/rcv.php?code=" + Nsm_Code;
        }
    }

    sidebar.btnEvent = {
        favor: function ($this) {
            var exceptPage = $('#exceptPage').val();

            if (!(exceptPage === "/starter" || exceptPage === "/starter/calendar")) {
                var $btnThis = $this;

                var memSys = $btnThis.data("memSys");
                var loginObj = sidebar.event.loginCheck();

                if (loginObj) {
                    var favorStat = $btnThis.hasClass('tplBtnFavOff') ? 1 : 0; // 현재 상태

                    // 비로그인일 경우 로그인 창 띄움
                    if (loginObj.RtnCode < 0) {
                        sidebar.event.loginOpen();
                    } else if (loginObj.MemChk == "1") {
                        var afterClass = favorStat == 1 ? "tplBtnFavOn" : "tplBtnFavOff";

                        $.getScript("/resources/lib/jkmon/dist/jk.pc.min.js", function (data, textStatus, jqxhr) {
                            $.getScript("/resources/js/user/dist/jk.user.common.min.js", function (data, textStatus, jqxhr) {
                                jk.user.core.favorCo({ memSysNo: memSys, favorState: favorStat }
                                    , (function (result) {
                                        if (result.code === 1 && result.memSysNoList != null) {
                                            result.memSysNoList.forEach(function (item) {
                                                var $buttons = $btnThis.closest('ul').find('button[data-mem-sys="' + item + '"]');
                                                $buttons.removeClass("tplBtnFavOn").removeClass("tplBtnFavOff").addClass(afterClass);
                                            });

                                        } else {
                                            alert(result.msg);
                                        }
                                    })
                                );
                            });
                        });

                    } else if (loginObj.MemChk > 0 && loginObj.MemChk != "1") {
                        alert("'관심기업 설정'은 개인회원 로그인 후 사용가능 합니다.");
                    }
                }
            }
        },
        scrap: function ($this) {
            var $self = $this;
            var giNo = $self.data("gino");
            var gno = $self.data("gno");
            var memberId = $self.data("m_id");
            var memberType = $self.data("membertype");
            var state = $self.is('.on') ? 0 : 1; // 현재 상태        

            // 로그인 체크
            var loginObj = sidebar.event.loginCheck();

            if (loginObj) {
                // 비로그인일 경우 로그인 창 띄움
                if (loginObj.RtnCode < 0) {
                    sidebar.event.loginOpen();
                } else if (loginObj.MemChk == "1") {
                    // 스크랩 해제
                    if (state == 0) {
                        var data = {
                            GI_No: giNo,
                            Gno: gno,
                            rScrapStat: state,
                            Mem_ID: memberId,
                            Mem_Type_Code: memberType
                        };

                        $.ajax({
                            type: 'get',
                            url: '/Recruit/GI_Scrap_Ajax',
                            data: data,
                            contentType: 'application/json; charset=utf-8',
                            dataType: 'json',
                            cache: false,
                            success: function (result) {
                                $self.removeClass('on');
                                $self.removeClass('tplBtnScrOn');
                                $self.addClass('tplBtnScrOff');

                                //sidebar.event.NsmConversionClick('170'); //Nsm 스크립트 추가

                                if (result == '1') {
                                    //_LA.EVT('4031');
                                    sidebar.event.go_event_icon_view(); //채용정보 개편 이벤트 아이콘
                                    sidebar.event.Criteo(giNo); //크리테오, 타게팅게이츠
                                    try {
                                        if (typeof kakaoPixelTag !== 'undefined') {
                                            kakaoPixelTag.method().addToCart();
                                        }
                                    } catch (ex) { }
                                }
                            }
                        });
                    }
                    else {
                        var data = { rScrapStat: state, Mem_ID: memberId };

                        $.ajax({   //채용공고 스크랩 수 체크 1일 : 100건 총 600건 이하로 제한
                            type: 'get',
                            url: '/Recruit/GI_Scrap_Limit_Ajax',
                            data: data,
                            cache: false,
                            success: function (result) {
                                if (result == '2') {
                                    alert('채용공고 스크랩은 1일 1,000건까지 가능합니다.');
                                }
                                else if (result == '3') {
                                    alert('채용공고 스크랩은 최대 6,000건까지 가능합니다.');
                                }
                                else {
                                    var data = {
                                        GI_No: giNo,
                                        Gno: gno,
                                        rScrapStat: state,
                                        Mem_ID: memberId,
                                        Mem_Type_Code: memberType
                                    };

                                    $.ajax({
                                        type: 'get',
                                        url: '/Recruit/GI_Scrap_Ajax',
                                        data: data,
                                        contentType: 'application/json; charset=utf-8',
                                        dataType: 'json',
                                        cache: false,
                                        success: function (result) {
                                            if (state == 0) {
                                                $self.removeClass('on');
                                                $self.removeClass('tplBtnScrOn');
                                                $self.addClass('tplBtnScrOff');
                                            }
                                            else {
                                                $self.addClass('on');
                                                $self.addClass('tplBtnScrOn');
                                                $self.removeClass('tplBtnScrOff');
                                            }


                                            //sidebar.event.NsmConversionClick('170'); //Nsm 스크립트 추가

                                            if (result == '1') {
                                                //_LA.EVT('4031');
                                                sidebar.event.go_event_icon_view(); //채용정보 개편 이벤트 아이콘
                                                sidebar.event.Criteo(giNo); //크리테오, 타게팅게이츠
                                                try {
                                                    if (typeof kakaoPixelTag !== 'undefined') {
                                                        kakaoPixelTag.method().addToCart();
                                                    }
                                                } catch (ex) { }
                                            }
                                        }
                                    });
                                }
                            }
                        });
                    }

                } else if (loginObj.MemChk > 0 && loginObj.MemChk != "1") {
                    alert("'스크랩'은 개인회원 로그인 후 사용가능 합니다.");
                }
            }
        }
    };

    var btnEvent = function () {
        $.each(BtnEls, function (k, v) {
            if (BtnEls.hasOwnProperty(k)) {
                $(document).on("click", v, function (e) {
                    sidebar.btnEvent[k]($(this));
                });
            }
        });
    };

    return sidebar;

})(window.sidebar || {}, jQuery); //객체 없으면 생성

$(document).ready(function () {
    sidebar.init.exe();
});

