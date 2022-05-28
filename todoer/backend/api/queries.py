import json
from .models import Todo
from ariadne import convert_kwargs_to_snake_case

# Grapqhl resolvers for queries
def resolve_todos(obj, info):
    try:
        todos = [todo.to_dict() for todo in Todo.query.all()]
        payload = {
            "success": True,
            "todos": todos
        }
    except Exception as error:
        payload = {
            "success": False,
            "errors": [str(error)]
        }
    return payload

@convert_kwargs_to_snake_case
def resolve_todo(obj, info, todo_id):
    try:
        todo = Todo.query.get(todo_id)
        payload = {
            "success": True,
            "todo": todo.to_dict()
        }

    except AttributeError:  # todo not found
        payload = {
            "success": False,
            "errors": [f"Todo item matching id {todo_id} not found"]
        }

    return payload

def resolve_embeddings(obj, info):
     payload = {
         'data': json.dumps([
                        [
                            -0.36499756446397,
                            -0.9535619122565171,
                            {
                                "class": "forest",
                                "type": "production",
                                "ml_model_version": "v2"
                            }
                        ],
                        [
                            -0.29495970537754035,
                            -0.9451865762368756,
                            {
                                "class": "sky",
                                "type": "test",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            -0.8156362038084128,
                            -0.9156668236926366,
                            {
                                "class": "buildings",
                                "type": "test",
                                "ml_model_version": "v2"
                            }
                        ],
                        [
                            0.32185841086298783,
                            -0.5201570918560843,
                            {
                                "class": "buildings",
                                "type": "triage",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            -0.29632777920567266,
                            -0.0069960164283626725,
                            {
                                "class": "sky",
                                "type": "test",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            -0.5502095560633924,
                            0.36823586093190697,
                            {
                                "class": "buildings",
                                "type": "production",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            0.6429260968973973,
                            0.8427815166759465,
                            {
                                "class": "buildings",
                                "type": "test",
                                "ml_model_version": "v1"
                            }
                        ],
                        [
                            -0.2025677072607901,
                            0.019140476146100927,
                            {
                                "class": "forest",
                                "type": "test",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            0.09840924912460425,
                            -0.7141131591823844,
                            {
                                "class": "buildings",
                                "type": "test",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            0.36759974971336007,
                            -0.7578820571150806,
                            {
                                "class": "forest",
                                "type": "production",
                                "ml_model_version": "v1"
                            }
                        ],
                        [
                            -0.1477070508815408,
                            0.14976541918620967,
                            {
                                "class": "buildings",
                                "type": "triage",
                                "ml_model_version": "v2"
                            }
                        ],
                        [
                            0.4322587619530083,
                            -0.6871516208722874,
                            {
                                "class": "buildings",
                                "type": "test",
                                "ml_model_version": "v1"
                            }
                        ],
                        [
                            0.7606019900860255,
                            -0.23109630969498385,
                            {
                                "class": "sky",
                                "type": "triage",
                                "ml_model_version": "v1"
                            }
                        ],
                        [
                            0.3486994109039032,
                            -0.8848551325652312,
                            {
                                "class": "sky",
                                "type": "production",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            0.5660979694341632,
                            -0.09520886974539078,
                            {
                                "class": "buildings",
                                "type": "test",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            -0.3284423636170555,
                            -0.8654241643208254,
                            {
                                "class": "forest",
                                "type": "production",
                                "ml_model_version": "v1"
                            }
                        ],
                        [
                            0.7905584163583468,
                            -0.23383196273448625,
                            {
                                "class": "buildings",
                                "type": "test",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            0.5021868768210065,
                            -0.8556494309954692,
                            {
                                "class": "forest",
                                "type": "production",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            -0.6332670156808407,
                            -0.2805534266822103,
                            {
                                "class": "forest",
                                "type": "test",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            0.49490265847614046,
                            0.9158985706450387,
                            {
                                "class": "sky",
                                "type": "production",
                                "ml_model_version": "v2"
                            }
                        ],
                        [
                            0.4300650549660672,
                            -0.3768340486182118,
                            {
                                "class": "sky",
                                "type": "triage",
                                "ml_model_version": "v1"
                            }
                        ],
                        [
                            -0.2057739088069961,
                            0.3258988766895947,
                            {
                                "class": "buildings",
                                "type": "production",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            0.232180784243452,
                            -0.17917439266707635,
                            {
                                "class": "forest",
                                "type": "triage",
                                "ml_model_version": "v2"
                            }
                        ],
                        [
                            -0.9937132485493314,
                            0.9062584654303119,
                            {
                                "class": "buildings",
                                "type": "production",
                                "ml_model_version": "v2"
                            }
                        ],
                        [
                            0.6749453111713217,
                            -0.8975080569301372,
                            {
                                "class": "forest",
                                "type": "production",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            0.6595359407623826,
                            0.8719710231578897,
                            {
                                "class": "sky",
                                "type": "triage",
                                "ml_model_version": "v1"
                            }
                        ],
                        [
                            0.9873280589157747,
                            0.08452878624669058,
                            {
                                "class": "buildings",
                                "type": "test",
                                "ml_model_version": "v1"
                            }
                        ],
                        [
                            -0.9710964871777663,
                            -0.519566493140593,
                            {
                                "class": "buildings",
                                "type": "test",
                                "ml_model_version": "v1"
                            }
                        ],
                        [
                            -0.7316090101689272,
                            -0.5164940257394859,
                            {
                                "class": "buildings",
                                "type": "triage",
                                "ml_model_version": "v1"
                            }
                        ],
                        [
                            -0.515134138868607,
                            0.8168022227093568,
                            {
                                "class": "forest",
                                "type": "test",
                                "ml_model_version": "v1"
                            }
                        ],
                        [
                            0.7247364130859522,
                            0.7562432699121997,
                            {
                                "class": "buildings",
                                "type": "triage",
                                "ml_model_version": "v1"
                            }
                        ],
                        [
                            -0.4023718941627896,
                            -0.17493427759873947,
                            {
                                "class": "forest",
                                "type": "production",
                                "ml_model_version": "v1"
                            }
                        ],
                        [
                            -0.3473206170358045,
                            0.30844495386786086,
                            {
                                "class": "buildings",
                                "type": "test",
                                "ml_model_version": "v2"
                            }
                        ],
                        [
                            0.8201044949449119,
                            -0.7011690823354746,
                            {
                                "class": "sky",
                                "type": "production",
                                "ml_model_version": "v2"
                            }
                        ],
                        [
                            -0.6267109161745723,
                            0.14676997769328493,
                            {
                                "class": "buildings",
                                "type": "test",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            0.6266968625113329,
                            -0.309645300744267,
                            {
                                "class": "forest",
                                "type": "triage",
                                "ml_model_version": "v1"
                            }
                        ],
                        [
                            0.5169854388165587,
                            -0.843172073491115,
                            {
                                "class": "sky",
                                "type": "production",
                                "ml_model_version": "v2"
                            }
                        ],
                        [
                            0.2067993303979545,
                            0.19783225866352083,
                            {
                                "class": "buildings",
                                "type": "triage",
                                "ml_model_version": "v2"
                            }
                        ],
                        [
                            0.2148437582158942,
                            0.5627539238634944,
                            {
                                "class": "forest",
                                "type": "production",
                                "ml_model_version": "v1"
                            }
                        ],
                        [
                            -0.8593236223622784,
                            0.8434660419503834,
                            {
                                "class": "forest",
                                "type": "test",
                                "ml_model_version": "v2"
                            }
                        ],
                        [
                            0.805633820923461,
                            0.42691941586479354,
                            {
                                "class": "forest",
                                "type": "triage",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            -0.7569119334701675,
                            -0.03961833148212124,
                            {
                                "class": "forest",
                                "type": "test",
                                "ml_model_version": "v2"
                            }
                        ],
                        [
                            0.8289584053097383,
                            0.40926457228985447,
                            {
                                "class": "forest",
                                "type": "test",
                                "ml_model_version": "v1"
                            }
                        ],
                        [
                            0.5719860823056959,
                            0.769856551661813,
                            {
                                "class": "sky",
                                "type": "test",
                                "ml_model_version": "v2"
                            }
                        ],
                        [
                            -0.5348090878746947,
                            -0.7162727255076953,
                            {
                                "class": "sky",
                                "type": "production",
                                "ml_model_version": "v1"
                            }
                        ],
                        [
                            0.5975161911818296,
                            -0.1935993411785999,
                            {
                                "class": "sky",
                                "type": "production",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            0.5173757774959902,
                            -0.044238225681731524,
                            {
                                "class": "forest",
                                "type": "production",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            -0.33574975776615856,
                            -0.8642685538872716,
                            {
                                "class": "sky",
                                "type": "triage",
                                "ml_model_version": "v2"
                            }
                        ],
                        [
                            -0.39071617681027426,
                            -0.723251812795414,
                            {
                                "class": "forest",
                                "type": "triage",
                                "ml_model_version": "v1"
                            }
                        ],
                        [
                            -0.06751357242791034,
                            -0.03815220758570259,
                            {
                                "class": "sky",
                                "type": "test",
                                "ml_model_version": "v1"
                            }
                        ],
                        [
                            0.6210770373200765,
                            -0.7003431535475486,
                            {
                                "class": "buildings",
                                "type": "production",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            0.5313676055780285,
                            0.2217611556666932,
                            {
                                "class": "buildings",
                                "type": "production",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            0.8146602506697365,
                            -0.9620406163009716,
                            {
                                "class": "buildings",
                                "type": "production",
                                "ml_model_version": "v1"
                            }
                        ],
                        [
                            -0.34090139024496,
                            -0.4228467801238436,
                            {
                                "class": "sky",
                                "type": "production",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            -0.09518542993598178,
                            0.3189238018552345,
                            {
                                "class": "forest",
                                "type": "triage",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            -0.9277622583732321,
                            -0.8401725494008123,
                            {
                                "class": "sky",
                                "type": "test",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            0.31476746514044107,
                            0.9724573019989289,
                            {
                                "class": "buildings",
                                "type": "test",
                                "ml_model_version": "v1"
                            }
                        ],
                        [
                            0.5569119259863275,
                            0.9922895949202957,
                            {
                                "class": "forest",
                                "type": "test",
                                "ml_model_version": "v1"
                            }
                        ],
                        [
                            -0.07230398201616017,
                            0.2720560030560697,
                            {
                                "class": "sky",
                                "type": "triage",
                                "ml_model_version": "v1"
                            }
                        ],
                        [
                            -0.3811760855206998,
                            -0.574494985131949,
                            {
                                "class": "sky",
                                "type": "test",
                                "ml_model_version": "v2"
                            }
                        ],
                        [
                            -0.13860810060996442,
                            -0.48055137560778993,
                            {
                                "class": "buildings",
                                "type": "production",
                                "ml_model_version": "v1"
                            }
                        ],
                        [
                            0.3092907807483898,
                            -0.3956847859510444,
                            {
                                "class": "forest",
                                "type": "production",
                                "ml_model_version": "v1"
                            }
                        ],
                        [
                            0.1774606810582564,
                            -0.08682587530006858,
                            {
                                "class": "buildings",
                                "type": "triage",
                                "ml_model_version": "v2"
                            }
                        ],
                        [
                            0.9853742100381693,
                            -0.8200406419857202,
                            {
                                "class": "sky",
                                "type": "test",
                                "ml_model_version": "v1"
                            }
                        ],
                        [
                            -0.12706699053211823,
                            0.8452584136090846,
                            {
                                "class": "forest",
                                "type": "production",
                                "ml_model_version": "v1"
                            }
                        ],
                        [
                            -0.8078093425302408,
                            0.8884848675905759,
                            {
                                "class": "forest",
                                "type": "production",
                                "ml_model_version": "v2"
                            }
                        ],
                        [
                            -0.19001523398844888,
                            0.43487869555099357,
                            {
                                "class": "sky",
                                "type": "production",
                                "ml_model_version": "v2"
                            }
                        ],
                        [
                            0.17503466694174907,
                            -0.6537685546162466,
                            {
                                "class": "buildings",
                                "type": "triage",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            -0.9513361196384404,
                            0.9271674735715774,
                            {
                                "class": "buildings",
                                "type": "production",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            -0.9527033228941932,
                            0.2735098520170669,
                            {
                                "class": "forest",
                                "type": "production",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            -0.24996907789744016,
                            -0.1757105216480177,
                            {
                                "class": "buildings",
                                "type": "triage",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            -0.666820678581006,
                            -0.8048494918027753,
                            {
                                "class": "forest",
                                "type": "production",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            -0.45386552674307934,
                            0.8437124000772203,
                            {
                                "class": "buildings",
                                "type": "production",
                                "ml_model_version": "v1"
                            }
                        ],
                        [
                            0.5366699602047058,
                            0.10919237582817987,
                            {
                                "class": "sky",
                                "type": "triage",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            0.7869262192542426,
                            -0.6814226725271442,
                            {
                                "class": "buildings",
                                "type": "triage",
                                "ml_model_version": "v2"
                            }
                        ],
                        [
                            0.9448180869490881,
                            0.20303584750115578,
                            {
                                "class": "sky",
                                "type": "test",
                                "ml_model_version": "v1"
                            }
                        ],
                        [
                            -0.23737970511598094,
                            0.5615814972801156,
                            {
                                "class": "forest",
                                "type": "test",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            0.7539260507038845,
                            -0.5628936198721934,
                            {
                                "class": "forest",
                                "type": "production",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            -0.8704506167045318,
                            0.3157542832062967,
                            {
                                "class": "sky",
                                "type": "test",
                                "ml_model_version": "v2"
                            }
                        ],
                        [
                            -0.7536181311244241,
                            -0.37831612803035064,
                            {
                                "class": "buildings",
                                "type": "triage",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            -0.5665752541181321,
                            -0.046764206486805904,
                            {
                                "class": "forest",
                                "type": "triage",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            -0.8144422792721793,
                            -0.15744071794625514,
                            {
                                "class": "buildings",
                                "type": "triage",
                                "ml_model_version": "v2"
                            }
                        ],
                        [
                            0.11791109756108975,
                            -0.8353545515513421,
                            {
                                "class": "forest",
                                "type": "triage",
                                "ml_model_version": "v1"
                            }
                        ],
                        [
                            -0.43101741090168044,
                            -0.015105177480974952,
                            {
                                "class": "forest",
                                "type": "production",
                                "ml_model_version": "v2"
                            }
                        ],
                        [
                            0.5381686971653443,
                            -0.7706550688792522,
                            {
                                "class": "sky",
                                "type": "production",
                                "ml_model_version": "v2"
                            }
                        ],
                        [
                            0.13099204731602176,
                            0.003110286902282766,
                            {
                                "class": "forest",
                                "type": "test",
                                "ml_model_version": "v1"
                            }
                        ],
                        [
                            0.6014346063051894,
                            -0.008928854260910057,
                            {
                                "class": "buildings",
                                "type": "test",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            0.35177976401697997,
                            -0.6608123072543188,
                            {
                                "class": "buildings",
                                "type": "test",
                                "ml_model_version": "v2"
                            }
                        ],
                        [
                            -0.050350899631798285,
                            -0.27543919351731905,
                            {
                                "class": "buildings",
                                "type": "test",
                                "ml_model_version": "v2"
                            }
                        ],
                        [
                            -0.08989119741197493,
                            -0.38543854292861246,
                            {
                                "class": "sky",
                                "type": "test",
                                "ml_model_version": "v2"
                            }
                        ],
                        [
                            -0.9758399312243657,
                            -0.8748689856997709,
                            {
                                "class": "sky",
                                "type": "production",
                                "ml_model_version": "v1"
                            }
                        ],
                        [
                            0.6254126199145671,
                            -0.30990706865087514,
                            {
                                "class": "buildings",
                                "type": "triage",
                                "ml_model_version": "v1"
                            }
                        ],
                        [
                            -0.6110864279992878,
                            -0.744640792513692,
                            {
                                "class": "buildings",
                                "type": "triage",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            0.5480949670351825,
                            -0.8313333755370884,
                            {
                                "class": "sky",
                                "type": "triage",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            -0.2775375674289795,
                            -0.7327083330042541,
                            {
                                "class": "buildings",
                                "type": "test",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            0.08672055042291626,
                            0.44679828876703764,
                            {
                                "class": "sky",
                                "type": "triage",
                                "ml_model_version": "v1"
                            }
                        ],
                        [
                            -0.10153710191108001,
                            0.7439161383993986,
                            {
                                "class": "sky",
                                "type": "production",
                                "ml_model_version": "v2"
                            }
                        ],
                        [
                            -0.20284969923354934,
                            -0.7765775084109126,
                            {
                                "class": "buildings",
                                "type": "test",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            0.2411797531143014,
                            -0.36340439504242106,
                            {
                                "class": "buildings",
                                "type": "triage",
                                "ml_model_version": "v3"
                            }
                        ],
                        [
                            -0.08962248364024372,
                            -0.010286640184160145,
                            {
                                "class": "buildings",
                                "type": "test",
                                "ml_model_version": "v2"
                            }
                        ]
                    ])
     }
     return payload